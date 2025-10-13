"""Worker that powers the durable ETL demo workflow."""

from __future__ import annotations

import asyncio
from typing import Sequence

from temporalio.client import Client
from temporalio.contrib.pydantic import pydantic_data_converter
from temporalio.worker import Worker

from .activities import (
    enumerate_batches,
    fetch_http_batch,
    fetch_object_batch,
    load_batch,
    transform_batch,
)
from .etl_workflow import DurableEtlWorkflow

DEFAULT_TASK_QUEUE = "etl-task-queue"


async def start_worker(task_queue: str = DEFAULT_TASK_QUEUE) -> None:
    """Start the Temporal worker that registers workflows and activities."""
    client = await Client.connect(
        "localhost:7233",
        data_converter=pydantic_data_converter,
    )
    activities: Sequence = (
        enumerate_batches,
        fetch_http_batch,
        fetch_object_batch,
        transform_batch,
        load_batch,
    )
    async with Worker(
        client,
        task_queue=task_queue,
        workflows=[DurableEtlWorkflow],
        activities=list(activities),
    ):
        await asyncio.Event().wait()


async def main() -> None:  # pragma: no cover
    """Entry point used by `uv run -m src.workflows.etl.worker`."""
    await start_worker()


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
