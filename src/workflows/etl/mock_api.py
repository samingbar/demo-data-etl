"""Synthetic HTTP API used to demonstrate resilient ETL ingestion."""

from __future__ import annotations

import asyncio
import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional

from aiohttp import web


@dataclass
class MockApiConfig:
    """Runtime configuration for the mock API server."""

    host: str = "127.0.0.1"
    port: int = 8081
    dataset_size: int = 200
    base_failure_seed: str = "temporal-etl-demo"
    rate_limit_window_seconds: int = 8
    timeout_seconds: int = 6


class MockApiServer:
    """Simple aiohttp server that simulates an unreliable upstream service."""

    def __init__(self, config: MockApiConfig | None = None) -> None:
        self.config = config or MockApiConfig()
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._gates: Dict[str, datetime] = {}
        self._datasets: Dict[str, List[dict]] = {}
        self._seed_dataset("transactions")

    async def start(self) -> None:
        """Start the aiohttp web server."""
        app = web.Application()
        app.add_routes([web.get("/datasets/{dataset}", self._handle_dataset)])
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        self._site = web.TCPSite(
            self._runner, self.config.host, self.config.port
        )
        await self._site.start()

    @property
    def bound_port(self) -> int:
        """Return the actual port the server is bound to."""
        if self._site and self._site._server and self._site._server.sockets:
            return self._site._server.sockets[0].getsockname()[1]
        return self.config.port

    async def stop(self) -> None:
        """Stop the server gracefully."""
        if self._site is not None:
            await self._site.stop()
        if self._runner is not None:
            await self._runner.cleanup()

    async def _handle_dataset(self, request: web.Request) -> web.Response:
        dataset = request.match_info["dataset"]
        page = int(request.query.get("page", "1"))
        page_size = int(request.query.get("page_size", "50"))
        key = f"{dataset}:{page}"
        gate_until = self._gates.get(dataset)
        now = datetime.now(tz=timezone.utc)
        if gate_until and now < gate_until:
            return web.json_response(
                {"detail": "rate limit window active", "retry_after": 2},
                status=429,
            )
        failure = self._calculate_failure(key)
        if failure == "timeout":
            await asyncio.sleep(self.config.timeout_seconds + 1)
        elif failure in ("429", "503"):
            status = int(failure)
            message = "rate limited" if status == 429 else "upstream overload"
            if status == 429:
                self._gates[dataset] = now + timedelta(
                    seconds=self.config.rate_limit_window_seconds
                )
            return web.json_response(
                {"detail": message, "retry_after": 2},
                status=status,
            )
        records = self._datasets.get(dataset)
        if records is None:
            return web.json_response(
                {"detail": f"dataset {dataset} not found"},
                status=404,
            )
        total_pages = max(1, (len(records) + page_size - 1) // page_size)
        if page > total_pages:
            return web.json_response(
                {"detail": f"page {page} beyond available data"},
                status=404,
            )
        start = (page - 1) * page_size
        end = min(start + page_size, len(records))
        items = records[start:end]
        response = {
            "batch_id": f"{dataset}-p{page}",
            "dataset": dataset,
            "page": page,
            "page_size": page_size,
            "total_pages": total_pages,
            "items": items,
        }
        return web.json_response(response)

    def _calculate_failure(self, key: str) -> Optional[str]:
        """Compute if a synthetic failure should be injected for the request."""
        digest = hashlib.sha256(f"{key}:{self.config.base_failure_seed}".encode("utf-8")).hexdigest()
        bucket = int(digest[:2], 16)
        if bucket % 17 == 0:
            return "timeout"
        if bucket % 11 == 0:
            return "429"
        if bucket % 13 == 0:
            return "503"
        return None

    def _seed_dataset(self, name: str) -> None:
        """Generate deterministic dataset rows for the mock API."""
        if name in self._datasets:
            return
        rows: List[dict] = []
        for idx in range(1, self.config.dataset_size + 1):
            currency = ["USD", "EUR", "GBP", "JPY"][idx % 4]
            country = ["US", "DE", "GB", "JP", "CA"][idx % 5]
            record = {
                "id": f"{name}-{idx}",
                "customer_id": f"CUST-{idx:04d}",
                "amount": round(25 + (idx % 17) * 3.14, 2),
                "currency": currency,
                "country": country,
                "email": f"user{idx}@example.com",
                "status": "completed" if idx % 3 else "pending",
                "created_at": datetime.now(tz=timezone.utc).isoformat(),
                "source_system": "http_api",
            }
            rows.append(record)
        self._datasets[name] = rows


async def run_forever(config: MockApiConfig | None = None) -> None:  # pragma: no cover
    """Run the mock API server until cancelled."""
    server = MockApiServer(config)
    await server.start()
    try:
        await asyncio.Event().wait()
    finally:
        await server.stop()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(run_forever())
