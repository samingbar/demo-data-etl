"""Command line helpers for running the Temporal Durable ETL demo."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import shutil
import signal
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime
import aiohttp
from aiohttp import ClientError, ClientSession

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
 

from src.workflows.etl.activities import (
    enumerate_batches,
    fetch_http_batch,
    fetch_object_batch,
    load_batch,
    transform_batch,
)
from src.workflows.etl.etl_workflow import DurableEtlWorkflow
from src.workflows.etl.mock_api import MockApiConfig, MockApiServer
from src.workflows.etl.models import (
    EtlWorkflowInput,
    HttpSourceConfig,
    ObjectStorageSourceConfig,
    SinkConfig,
    SourceType,
)
from src.workflows.etl.worker import DEFAULT_TASK_QUEUE

STATE_DIR = Path(".demo")
STATE_FILE = STATE_DIR / "state.json"
FIRST_RUN_SENTINEL = STATE_DIR / "first_run_done"


@dataclass
class DemoState:
    """Metadata about a single in-flight demo workflow run."""

    workflow_id: str
    run_id: Optional[str]
    task_queue: str
    source_type: str
    created_at: float

    def to_json(self) -> Dict[str, Any]:
        return self.__dict__

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "DemoState":
        return cls(
            workflow_id=data.get("workflow_id"),
            run_id=data.get("run_id"),
            task_queue=data.get("task_queue"),
            source_type=data.get("source_type"),
            created_at=data.get("created_at", time.time()),
        )


@dataclass
class DemoIndex:
    """Global index of demo state allowing multiple concurrent runs."""

    # Shared services
    temporal_pid: Optional[int] = None
    api_host: Optional[str] = None
    api_port: Optional[int] = None
    created_at: float = field(default_factory=time.time)
    # Map workflow_id -> DemoState
    runs: Dict[str, DemoState] = field(default_factory=dict)

    def to_json(self) -> Dict[str, Any]:
        return {
            "temporal_pid": self.temporal_pid,
            "api_host": self.api_host,
            "api_port": self.api_port,
            "created_at": self.created_at,
            "runs": {k: v.to_json() for k, v in self.runs.items()},
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "DemoIndex":
        # Back-compat: if a single-run shape is detected, wrap it in the index
        if "runs" not in data:
            # Assume legacy DemoState schema
            legacy = DemoState.from_json(data)
            idx = cls(
                temporal_pid=data.get("temporal_pid"),
                api_host=data.get("api_host"),
                api_port=data.get("api_port"),
                created_at=data.get("created_at", time.time()),
                runs={legacy.workflow_id: legacy},
            )
            return idx
        runs = {k: DemoState.from_json(v) for k, v in data.get("runs", {}).items()}
        return cls(
            temporal_pid=data.get("temporal_pid"),
            api_host=data.get("api_host"),
            api_port=data.get("api_port"),
            created_at=data.get("created_at", time.time()),
            runs=runs,
        )


def load_index() -> Optional[DemoIndex]:
    if not STATE_FILE.exists():
        return None
    return DemoIndex.from_json(json.loads(STATE_FILE.read_text()))


def save_index(index: DemoIndex) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(index.to_json(), indent=2))


def clear_state() -> None:
    if STATE_FILE.exists():
        STATE_FILE.unlink()


async def launch_temporal_server(log_dir: Path) -> asyncio.subprocess.Process:
    """Launch the Temporal dev server."""
    log_dir.mkdir(parents=True, exist_ok=True)
    cmd = [
        "temporal",
        "server",
        "start-dev",
        "--log-format",
        "pretty",
        "--db-filename",
        str(log_dir / "temporal.db"),
    ]
    try:
        process = await asyncio.create_subprocess_exec(*cmd)
    except FileNotFoundError as exc:  # pragma: no cover - depends on local setup
        raise RuntimeError(
            "Temporal CLI not found. Please install it from https://docs.temporal.io/cli before running the demo."
        ) from exc
    return process


async def wait_for_server(host: str = "localhost:7233", retries: int = 30) -> None:
    """Wait until the Temporal server responds to client connections."""
    for attempt in range(1, retries + 1):
        try:
            client = await Client.connect(host)
        except Exception:
            await asyncio.sleep(1)
            continue


async def start_demo(args: argparse.Namespace) -> None:
    """Start the end-to-end demo run."""
    # Allow multiple concurrent runs; reuse shared services if present.
    index = load_index()
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    log_dir = STATE_DIR / "logs"
    temporal_proc: Optional[asyncio.subprocess.Process] = None
    # Launch Temporal dev server only if this is the first run
    if index is None or index.temporal_pid is None:
        temporal_proc = await launch_temporal_server(log_dir)
    workflow_id = f"etl-demo-{int(time.time())}"
    mock_api: Optional[MockApiServer] = None
    client: Optional[Client] = None
    demo_started = False
    try:
        await wait_for_server()
        # Determine whether to inject failures in the mock API.
        # Default behavior: disable on the very first run, enable otherwise.
        if args.inject_failures is None:
            default_inject = FIRST_RUN_SENTINEL.exists()
        else:
            default_inject = bool(args.inject_failures)

        api_config = MockApiConfig(port=args.api_port, inject_failures=default_inject)

        async def _api_is_running(host: str, port: int) -> bool:
            url = f"http://{host}:{port}/_admin/mock/status"
            try:
                timeout = aiohttp.ClientTimeout(total=2)
                async with ClientSession(timeout=timeout) as session:
                    async with session.get(url) as resp:
                        return resp.status == 200
            except Exception:
                return False

        if args.source == "http":
            # If an API location is recorded, verify it is actually reachable; otherwise launch it.
            if index is not None and index.api_host is not None and index.api_port is not None:
                # Enforce stable API host/port across concurrent runs
                if index.api_host != api_config.host or index.api_port != api_config.port:
                    raise RuntimeError(
                        f"Mock API already running at {index.api_host}:{index.api_port}; cannot start with different host/port {api_config.host}:{api_config.port}."
                    )
                if await _api_is_running(index.api_host, index.api_port):
                    mock_api = None  # Reuse existing
                else:
                    # Recorded endpoint is down; start a new one at the same host/port
                    resurrect_cfg = MockApiConfig(
                        host=index.api_host,
                        port=index.api_port,
                        inject_failures=default_inject,
                    )
                    mock_api = MockApiServer(resurrect_cfg)
                    await mock_api.start()
            else:
                mock_api = MockApiServer(api_config)
                await mock_api.start()
        client = await Client.connect(
            "localhost:7233",
            namespace="default",
            data_converter=pydantic_data_converter,
        )
        http_config = None
        actual_port = api_config.port
        if mock_api is not None:
            actual_port = mock_api.bound_port
        if args.source == "http":
            http_config = HttpSourceConfig(
                base_url=f"http://{api_config.host}:{actual_port}",
                page_size=args.page_size,
                max_pages=args.max_pages,
            )
        object_config = ObjectStorageSourceConfig()
        sink = SinkConfig(sqlite_path=args.sink_path)
        workflow_input = EtlWorkflowInput(
            run_id=workflow_id,
            source_type=SourceType.HTTP_API if args.source == "http" else SourceType.OBJECT_STORAGE,
            http_source=http_config if args.source == "http" else None,
            object_source=object_config if args.source == "object" else None,
            sink=sink,
            max_concurrent_batches=args.max_concurrency,
        )

        # Ensure workflow_id uniqueness if a custom scheme is introduced in future
        if index and workflow_id in index.runs:
            raise RuntimeError(f"A workflow with id {workflow_id} already exists.")

        handle = await client.start_workflow(
            DurableEtlWorkflow.run,
            workflow_input,
            id=workflow_id,
            task_queue=DEFAULT_TASK_QUEUE,
        )
        # Update and persist the index
        if index is None:
            index = DemoIndex()
        # Set shared services details if not already set
        if index.temporal_pid is None and temporal_proc and temporal_proc.pid:
            index.temporal_pid = temporal_proc.pid
        if args.source == "http" and (index.api_host is None or index.api_port is None):
            index.api_host = api_config.host
            index.api_port = actual_port
        # Add this run
        index.runs[workflow_id] = DemoState(
            workflow_id=workflow_id,
            run_id=handle.run_id,
            task_queue=DEFAULT_TASK_QUEUE,
            source_type=workflow_input.source_type.value,
            created_at=time.time(),
        )
        save_index(index)
        # Mark that a first run has occurred so subsequent runs default to enabled faults.
        try:
            FIRST_RUN_SENTINEL.touch(exist_ok=True)
        except Exception:
            pass  # Non-fatal if we cannot write the sentinel
        print(f"Started workflow {workflow_id} (run_id={handle.run_id})")
        print("Note: Worker is not auto-started. Run `make worker` in another terminal.")
        await monitor_progress(handle, args.interval)
        print("Temporal dev server is still running; use `make demo.clean` when you're ready to tear it down.")
        demo_started = True
    finally:
        if mock_api is not None:
            await mock_api.stop()
        if not demo_started:
            if temporal_proc is not None and temporal_proc.returncode is None:
                temporal_proc.send_signal(signal.SIGTERM)
                try:
                    await asyncio.wait_for(temporal_proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    temporal_proc.kill()
            clear_state()
        else:
            # If the Temporal process exited unexpectedly, remove state so clean doesn't fail later.
            if temporal_proc is not None and temporal_proc.returncode is not None:
                clear_state()


async def monitor_progress(handle, interval: int) -> None:
    """Periodically query workflow progress and print a status line."""
    last_snapshot = None
    result_task = asyncio.create_task(handle.result())
    try:
        while not result_task.done():
            snapshot = await handle.query(DurableEtlWorkflow.progress)
            line = (
                f"[{snapshot.last_updated.isoformat()}] "
                f"batches {snapshot.batches_completed}/{snapshot.total_batches} "
                f"items {snapshot.items_processed} retries {snapshot.retry_count} "
                f"paused={snapshot.paused}"
            )
            if snapshot.last_error:
                line += f" last_error={snapshot.last_error}"
            if last_snapshot and snapshot.retry_count > last_snapshot.retry_count:
                print("Retry attempts increased; Temporal auto-retrying transient failures.")
            print(line)
            last_snapshot = snapshot
            await asyncio.sleep(interval)
        result = await result_task
        print("Workflow completed successfully:")
        print(json.dumps(result.model_dump(mode='json' ), indent=2))
    except Exception as exc:  # pragma: no cover - defensive
        print(f"Workflow failed: {exc}", file=sys.stderr)
        raise


def _select_workflow_id(index: DemoIndex, workflow_id: Optional[str]) -> str:
    if workflow_id:
        if workflow_id not in index.runs:
            raise RuntimeError(f"Workflow id {workflow_id} not found in state.")
        return workflow_id
    if not index.runs:
        raise RuntimeError("No active demo runs found.")
    if len(index.runs) == 1:
        return next(iter(index.runs.keys()))
    raise RuntimeError("Multiple runs found; specify --workflow-id to disambiguate.")


async def send_signal(signal_name: str, workflow_id: Optional[str]) -> None:
    """Send a pause/resume signal to a chosen workflow."""
    index = load_index()
    if index is None:
        raise RuntimeError("No active demo runs found.")
    target_wid = _select_workflow_id(index, workflow_id)
    client = await Client.connect(
        "localhost:7233",
        namespace="default",
        data_converter=pydantic_data_converter,
    )
    handle = client.get_workflow_handle(target_wid)
    if signal_name == "pause":
        await handle.signal(DurableEtlWorkflow.pause)
        print("Pause signal sent.")
    elif signal_name == "resume":
        await handle.signal(DurableEtlWorkflow.resume)
        print("Resume signal sent.")
    else:
        raise ValueError(f"Unknown signal {signal_name}")


async def query_status(workflow_id: Optional[str]) -> None:
    """Query a workflow for current progress."""
    index = load_index()
    if index is None:
        raise RuntimeError("No active demo runs found.")
    target_wid = _select_workflow_id(index, workflow_id)
    client = await Client.connect(
        "localhost:7233",
        namespace="default",
        data_converter=pydantic_data_converter,
    )
    handle = client.get_workflow_handle(target_wid)
    snapshot = await handle.query(DurableEtlWorkflow.progress)
    print(json.dumps(snapshot.model_dump(mode='json'), indent=2))


async def control_mock_api(online: bool, duration: Optional[int] = None) -> None:
    """Toggle the mock API server to simulate outages."""
    index = load_index()
    if index is None:
        raise RuntimeError("No active demo runs found.")
    # Mock API controls only make sense if HTTP demo server is managed
    if index.api_host is None or index.api_port is None:
        raise RuntimeError("Mock API controls are only available for HTTP source demo runs.")
    if not online and duration is not None and duration < 0:
        raise ValueError("duration must be non-negative")
    payload: Dict[str, Any] = {}
    if not online and duration:
        payload["duration_seconds"] = duration
    url_suffix = "online" if online else "offline"
    base_url = f"http://{index.api_host}:{index.api_port}"
    try:
        async with ClientSession() as session:
            async with session.post(f"{base_url}/_admin/mock/{url_suffix}", json=payload) as response:
                if response.status >= 400:
                    details = await response.text()
                    raise RuntimeError(
                        f"Mock API {url_suffix} request failed with HTTP {response.status}: {details}"
                    )
                data = await response.json()
    except ClientError as exc:
        raise RuntimeError(f"Failed to reach mock API control endpoint: {exc}") from exc

    if online:
        print("Mock API availability restored.")
    else:
        duration_info = data.get("duration_seconds")
        if duration_info:
            print(f"Mock API taken offline for {duration_info} seconds.")
        else:
            print("Mock API taken offline until restored with `mock online`.")


def clean_artifacts() -> None:
    """Remove generated artifacts and kill lingering Temporal dev servers."""
    index = load_index()

    # Helper: collect PIDs bound to TCP ports via lsof (best effort, POSIX only)
    def pids_on_ports(ports: list[int]) -> set[int]:
        pids: set[int] = set()
        if shutil.which("lsof") is None:
            return pids
        for port in ports:
            try:
                out = subprocess.check_output(["lsof", "-ti", f"tcp:{port}"] , text=True)
                for line in out.strip().splitlines():
                    line = line.strip()
                    if line.isdigit():
                        pids.add(int(line))
            except subprocess.CalledProcessError:
                # No processes on this port
                continue
            except Exception:
                # Any other failure: ignore and continue best-effort
                continue
        return pids

    # Helper: best-effort pattern kill (Temporal CLI dev server)
    def try_pkill(pattern: str) -> None:
        cmd = shutil.which("pkill")
        if not cmd:
            return
        try:
            subprocess.run([cmd, "-f", pattern], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except Exception:
            pass

    # Attempt to gracefully stop any tracked Temporal dev server
    if index and index.temporal_pid:
        try:
            os.kill(index.temporal_pid, signal.SIGTERM)
        except ProcessLookupError:
            pass

    # Aggressively clean up any stray Temporal dev servers and mock API servers
    # - Temporal dev server commonly uses 7233 (frontend) and 8233 (UI)
    # - Mock API defaults to 8081, but prefer the recorded port if present
    api_port = index.api_port if index and index.api_port else 8081
    candidate_ports = {7233, 8233, int(api_port)}
    pids = pids_on_ports(sorted(candidate_ports))

    # Also try name-based kill for Temporal CLI dev servers
    try_pkill("temporal server start-dev")

    # Send TERM to any discovered PIDs, then KILL if they linger
    for pid in list(pids):
        try:
            os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pids.discard(pid)
        except Exception:
            # Ignore and continue
            pass

    # Small grace period
    try:
        time.sleep(0.5)
    except Exception:
        pass

    # Recompute and force kill remaining if needed
    remaining = pids_on_ports(sorted(candidate_ports))
    for pid in list(remaining):
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except Exception:
            pass

    clear_state()
    if STATE_DIR.exists():
        shutil.rmtree(STATE_DIR)
    warehouse_path = Path("data/warehouse/warehouse.db")
    if warehouse_path.exists():
        warehouse_path.unlink()
    print("Demo artifacts removed.")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Temporal Durable ETL demo controller")
    subparsers = parser.add_subparsers(dest="command", required=True)

    start_parser = subparsers.add_parser("start", help="Launch the full demo stack")
    start_parser.add_argument("--source", choices=["http", "object"], default="http")
    start_parser.add_argument("--api-port", type=int, default=8081)
    start_parser.add_argument("--page-size", type=int, default=40)
    start_parser.add_argument("--max-pages", type=int, default=5)
    start_parser.add_argument("--max-concurrency", type=int, default=3)
    start_parser.add_argument("--interval", type=int, default=10, help="Progress poll interval (seconds)")
    start_parser.add_argument(
        "--sink-path",
        default="data/warehouse/warehouse.db",
        help="Location for the SQLite sink owned by the customer.",
    )
    # Failure injection control for the mock API: if neither is provided,
    # first run defaults to disabled, subsequent runs default to enabled.
    group = start_parser.add_mutually_exclusive_group()
    group.add_argument(
        "--enable-failures",
        dest="inject_failures",
        action="store_true",
        help="Enable simulated API failures (timeouts/429/503).",
    )
    group.add_argument(
        "--disable-failures",
        dest="inject_failures",
        action="store_false",
        help="Disable simulated API failures.",
    )
    start_parser.set_defaults(inject_failures=None)

    pause_parser = subparsers.add_parser("pause", help="Send pause signal to the workflow")
    resume_parser = subparsers.add_parser("resume", help="Send resume signal to the workflow")
    status_parser = subparsers.add_parser("status", help="Print workflow progress snapshot")
    # Targeting flags for multi-run control
    pause_parser.add_argument(
        "--workflow-id",
        dest="workflow_id",
        help="Target a specific workflow id when multiple runs exist.",
    )
    resume_parser.add_argument(
        "--workflow-id",
        dest="workflow_id",
        help="Target a specific workflow id when multiple runs exist.",
    )
    status_parser.add_argument(
        "--workflow-id",
        dest="workflow_id",
        help="Target a specific workflow id when multiple runs exist.",
    )
    subparsers.add_parser("clean", help="Stop services and remove artifacts")
    mock_parser = subparsers.add_parser("mock", help="Control the mock API availability")
    mock_subparsers = mock_parser.add_subparsers(dest="mock_command", required=True)
    mock_offline = mock_subparsers.add_parser("offline", help="Simulate a mock API outage")
    mock_offline.add_argument(
        "--duration",
        type=int,
        default=None,
        help="Optional outage duration in seconds (omit for manual restore).",
    )
    mock_subparsers.add_parser("online", help="Restore the mock API after an outage")

    return parser.parse_args(argv)


async def main(argv: list[str]) -> None:
    args = parse_args(argv)
    if args.command == "start":
        await start_demo(args)
    elif args.command == "pause":
        await send_signal("pause", getattr(args, "workflow_id", None))
    elif args.command == "resume":
        await send_signal("resume", getattr(args, "workflow_id", None))
    elif args.command == "status":
        await query_status(getattr(args, "workflow_id", None))
    elif args.command == "clean":
        clean_artifacts()
    elif args.command == "mock":
        if args.mock_command == "offline":
            await control_mock_api(False, args.duration)
        elif args.mock_command == "online":
            await control_mock_api(True)
        else:  # pragma: no cover - defensive
            raise RuntimeError(f"Unsupported mock API command {args.mock_command}")
    else:  # pragma: no cover - defensive
        raise RuntimeError(f"Unsupported command {args.command}")


if __name__ == "__main__":  # pragma: no cover
    try:
        asyncio.run(main(sys.argv[1:]))
    except KeyboardInterrupt:
        print("Demo interrupted.")
