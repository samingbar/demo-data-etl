"""Command line helpers for running the Temporal Durable ETL demo."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime
from aiohttp import ClientError, ClientSession
from pydantic import BaseModel

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

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


@dataclass
class DemoState:
    """Persisted metadata about an in-flight demo run."""

    workflow_id: str
    run_id: Optional[str]
    task_queue: str
    source_type: str
    temporal_pid: Optional[int]
    created_at: float
    api_host: Optional[str] = None
    api_port: Optional[int] = None

    def to_json(self) -> Dict[str, Any]:
        return self.__dict__

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "DemoState":
        return cls(**data)


def load_state() -> Optional[DemoState]:
    if not STATE_FILE.exists():
        return None
    return DemoState.from_json(json.loads(STATE_FILE.read_text()))


def save_state(state: DemoState) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state.to_json(), indent=2))


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
    if load_state() is not None:
        raise RuntimeError("An existing demo run is in progress; run `make demo.clean` first.")
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    log_dir = STATE_DIR / "logs"
    temporal_proc = await launch_temporal_server(log_dir)
    workflow_id = f"etl-demo-{int(time.time())}"
    mock_api: Optional[MockApiServer] = None
    client: Optional[Client] = None
    demo_started = False
    try:
        await wait_for_server()
        api_config = MockApiConfig(port=args.api_port)
        if args.source == "http":
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

        activities = [
            enumerate_batches,
            fetch_http_batch,
            fetch_object_batch,
            transform_batch,
            load_batch,
        ]

        async with Worker(
            client,
            task_queue=DEFAULT_TASK_QUEUE,
            workflows=[DurableEtlWorkflow],
            activities=activities,
        ):
            handle = await client.start_workflow(
                DurableEtlWorkflow.run,
                workflow_input,
                id=workflow_id,
                task_queue=DEFAULT_TASK_QUEUE,
            )
            save_state(
                DemoState(
                    workflow_id=workflow_id,
                    run_id=handle.run_id,
                    task_queue=DEFAULT_TASK_QUEUE,
                    source_type=workflow_input.source_type.value,
                    temporal_pid=temporal_proc.pid if temporal_proc.pid else None,
                    created_at=time.time(),
                    api_host=api_config.host if args.source == "http" else None,
                    api_port=actual_port if args.source == "http" else None,
                )
            )
            print(f"Started workflow {workflow_id} (run_id={handle.run_id})")
            await monitor_progress(handle, args.interval)
            print("Temporal dev server is still running; use `make demo.clean` when you're ready to tear it down.")
            demo_started = True
    finally:
        if mock_api is not None:
            await mock_api.stop()
        if not demo_started:
            if temporal_proc.returncode is None:
                temporal_proc.send_signal(signal.SIGTERM)
                try:
                    await asyncio.wait_for(temporal_proc.wait(), timeout=5)
                except asyncio.TimeoutError:
                    temporal_proc.kill()
            clear_state()
        else:
            # If the Temporal process exited unexpectedly, remove state so clean doesn't fail later.
            if temporal_proc.returncode is not None:
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


async def send_signal(signal_name: str) -> None:
    """Send a pause/resume signal to the active workflow."""
    state = load_state()
    if state is None:
        raise RuntimeError("No active demo run found.")
    client = await Client.connect(
        "localhost:7233",
        namespace="default",
        data_converter=pydantic_data_converter,
    )
    handle = client.get_workflow_handle(state.workflow_id)
    if signal_name == "pause":
        await handle.signal(DurableEtlWorkflow.pause)
        print("Pause signal sent.")
    elif signal_name == "resume":
        await handle.signal(DurableEtlWorkflow.resume)
        print("Resume signal sent.")
    else:
        raise ValueError(f"Unknown signal {signal_name}")


async def query_status() -> None:
    """Query the active workflow for current progress."""
    state = load_state()
    if state is None:
        raise RuntimeError("No active demo run found.")
    client = await Client.connect(
        "localhost:7233",
        namespace="default",
        data_converter=pydantic_data_converter,
    )
    handle = client.get_workflow_handle(state.workflow_id)
    snapshot = await handle.query(DurableEtlWorkflow.progress)
    print(json.dumps(snapshot.model_dump(mode='json'), indent=2))


async def control_mock_api(online: bool, duration: Optional[int] = None) -> None:
    """Toggle the mock API server to simulate outages."""
    state = load_state()
    if state is None:
        raise RuntimeError("No active demo run found.")
    if state.source_type != SourceType.HTTP_API.value:
        raise RuntimeError("Mock API controls are only available for HTTP source demo runs.")
    if state.api_host is None or state.api_port is None:
        raise RuntimeError("Mock API connection details missing; restart the demo.")
    if not online and duration is not None and duration < 0:
        raise ValueError("duration must be non-negative")
    payload: Dict[str, Any] = {}
    if not online and duration:
        payload["duration_seconds"] = duration
    url_suffix = "online" if online else "offline"
    base_url = f"http://{state.api_host}:{state.api_port}"
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
    state = load_state()
    if state and state.temporal_pid:
        try:
            os.kill(state.temporal_pid, signal.SIGTERM)
        except ProcessLookupError:
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

    subparsers.add_parser("pause", help="Send pause signal to the workflow")
    subparsers.add_parser("resume", help="Send resume signal to the workflow")
    subparsers.add_parser("status", help="Print workflow progress snapshot")
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
        await send_signal("pause")
    elif args.command == "resume":
        await send_signal("resume")
    elif args.command == "status":
        await query_status()
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
