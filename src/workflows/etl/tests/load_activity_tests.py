"""Unit tests covering sink idempotency for the load activity."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from temporalio.testing import ActivityEnvironment

from src.workflows.etl.activities import load_batch
from workflows.etl.models import LoadBatchRequest, SinkConfig, SourceBatch


@pytest.mark.asyncio
async def test_load_batch_is_idempotent(tmp_path: Path) -> None:
    """Writing the same records twice should not create duplicates."""
    env = ActivityEnvironment()
    sink = SinkConfig(sqlite_path=str(tmp_path / "warehouse.db"))
    batch = SourceBatch(batch_id="batch-1", sequence=1)
    records = [
        {
            "record_id": "rec-1",
            "normalized": {"example": "value", "id": "rec-1"},
            "checksum": "checksum-1",
        },
        {
            "record_id": "rec-2",
            "normalized": {"example": "value", "id": "rec-2"},
            "checksum": "checksum-2",
        },
    ]
    request = LoadBatchRequest(
        run_id="test-run",
        config=sink,
        batch=batch,
        records=records,
    )

    first_result = await env.run(load_batch, request)
    assert first_result.inserted_count == 2
    assert first_result.skipped_count == 0

    second_result = await env.run(load_batch, request)
    assert second_result.inserted_count == 0
    assert second_result.skipped_count == 2

    # Verify only two rows exist in the sink table
    sqlite_path = Path(sink.sqlite_path)
    import sqlite3

    with sqlite3.connect(sqlite_path) as conn:
        rows = conn.execute("SELECT record_id, normalized FROM etl_records").fetchall()
    assert len(rows) == 2
    payloads = {row[0]: json.loads(row[1]) for row in rows}
    assert payloads["rec-1"]["id"] == "rec-1"
    assert payloads["rec-2"]["id"] == "rec-2"
