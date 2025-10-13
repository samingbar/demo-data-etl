"""Integration tests for the durable ETL workflow."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest
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
from workflows.etl.models import EtlWorkflowInput, HttpSourceConfig, SinkConfig, SourceType


@pytest.mark.asyncio
async def test_etl_workflow_handles_retries_and_pause(env, tmp_path: Path) -> None:
    """Run end-to-end ETL flow and assert retries and idempotency."""
    server = MockApiServer(MockApiConfig(port=0))
    await server.start()
    task_queue = "test-etl-task-queue"
    sink_path = tmp_path / "warehouse.db"
    activities = [
        enumerate_batches,
        fetch_http_batch,
        fetch_object_batch,
        transform_batch,
        load_batch,
    ]
    input_data = EtlWorkflowInput(
        run_id="integration-run",
        source_type=SourceType.HTTP_API,
        http_source=HttpSourceConfig(
            base_url=f"http://127.0.0.1:{server.bound_port}",
            page_size=25,
            max_pages=4,
        ),
        sink=SinkConfig(sqlite_path=str(sink_path)),
        max_concurrent_batches=2,
    )
    result = None
    try:
        async with Worker(
            env.client,
            task_queue=task_queue,
            workflows=[DurableEtlWorkflow],
            activities=activities,
        ):
            handle = await env.client.start_workflow(
                DurableEtlWorkflow.run,
                input_data,
                id="etl-integration-test",
                task_queue=task_queue,
            )
            # Trigger pause/resume mid-flight to validate signals
            await asyncio.sleep(0.2)
            await handle.signal(DurableEtlWorkflow.pause)
            await asyncio.sleep(0.2)
            await handle.signal(DurableEtlWorkflow.resume)
            result = await handle.result()
    finally:
        await server.stop()

    assert result is not None
    assert result.retry_count > 0, "Transient API faults should trigger automatic retries"
    assert result.batches_completed == result.total_batches
    assert result.items_processed > 0

    with sqlite3.connect(sink_path) as conn:
        row = conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT record_id) FROM etl_records"
        ).fetchone()
    assert row[0] == row[1] == result.items_processed
