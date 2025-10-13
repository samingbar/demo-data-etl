"""Activities backing the durable ETL workflow demo."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import List
import random

import aiohttp
from aiohttp import ClientError, ClientTimeout
from temporalio import activity

from .errors import NonRetryableSourceError, RetryableSourceError, SinkWriteError
from .models import (
    EnumerateBatchesInput,
    ExtractBatchResult,
    ExtractedRecord,
    HttpBatchRequest,
    LoadBatchRequest,
    LoadBatchResult,
    ObjectBatchRequest,
    SinkConfig,
    SourceBatch,
    SourceType,
    TransformBatchRequest,
    TransformBatchResult,
)
from .transformations import normalize_records


def _ensure_sink(config: SinkConfig) -> None:
    """Ensure the SQLite sink exists before writes occur."""
    path = Path(config.sqlite_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {config.table_name} (
                record_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                batch_id TEXT NOT NULL,
                normalized JSON NOT NULL,
                checksum TEXT NOT NULL,
                ingested_at TEXT NOT NULL
            )
            """
        )
        conn.commit()


@activity.defn
async def enumerate_batches(input_data: EnumerateBatchesInput) -> List[SourceBatch]:
    """Enumerate batch descriptors for the workflow to fan out."""
    workflow_input = input_data.workflow_input
    if workflow_input.source_type is SourceType.HTTP_API:
        http_config = workflow_input.http_source
        assert http_config is not None  # Pydantic validator enforces this
        batches = [
            SourceBatch(
                batch_id=f"http-{idx}",
                sequence=idx,
                approx_item_count=http_config.page_size,
            )
            for idx in range(1, http_config.max_pages + 1)
        ]
        await asyncio.sleep(random.randint(1,25)) # Inject a pause into the execution for demo purposes 
        activity.logger.info(
            "Enumerated %s HTTP batches for dataset %s",
            len(batches),
            http_config.dataset,
        )
        return batches
    object_config = workflow_input.object_source
    assert object_config is not None
    source_dir = Path(object_config.root_path)
    if not source_dir.exists():
        raise NonRetryableSourceError(
            f"Object store path {source_dir} does not exist; nothing to ingest."
        )
    files = sorted(source_dir.glob(object_config.file_glob))
    if not files:
        raise NonRetryableSourceError(
            f"No batches found under {source_dir} matching {object_config.file_glob}"
        )
    batches: List[SourceBatch] = []
    for idx, file_path in enumerate(files):
        with file_path.open("r", encoding="utf-8") as handle:
            approx_count = sum(1 for _ in handle if _.strip())
        batches.append(
            SourceBatch(
                batch_id=file_path.name,
                sequence=idx + 1,
                approx_item_count=approx_count,
            )
        )
    activity.logger.info(
        "Enumerated %s object storage batches from %s",
        len(batches),
        source_dir,
    )
    await asyncio.sleep(random.randint(1,30)) # Inject a pause into the execution for demo purposes 

    return batches


async def _read_response_json(response: aiohttp.ClientResponse) -> dict:
    """Read and deserialize a JSON response while handling malformed payloads."""
    try:
        return await response.json()
    except aiohttp.ContentTypeError as exc:  # pragma: no cover - defensive
        text = await response.text()
        raise RetryableSourceError(
            f"Unexpected response content-type while fetching batch: {text}"
        ) from exc


@activity.defn
async def fetch_http_batch(request: HttpBatchRequest) -> ExtractBatchResult:
    """Fetch a page of records from the mock HTTP API."""
    config = request.config
    batch = request.batch
    timeout = ClientTimeout(total=config.request_timeout_seconds)
    params = {
        "page": batch.sequence,
        "page_size": config.page_size,
    }
    # Pydantic's HttpUrl appends a trailing slash, so strip it to avoid double slashes.
    base_url = str(config.base_url).rstrip("/")
    url = f"{base_url}/datasets/{config.dataset}"
    activity.logger.info(
        "Fetching HTTP batch %s (page=%s) from %s",
        batch.batch_id,
        batch.sequence,
        url,
    )
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url, params=params) as response:
                if response.status in (429, 503):
                    details = await response.text()
                    raise RetryableSourceError(
                        f"HTTP {response.status} while fetching batch {batch.batch_id}: {details}"
                    )
                if response.status == 404:
                    raise NonRetryableSourceError(
                        f"Dataset {config.dataset} not available at {url}"
                    )
                response.raise_for_status()
                payload = await _read_response_json(response)
    except asyncio.TimeoutError as exc:
        raise RetryableSourceError(
            f"Timeout after {timeout.total} seconds fetching batch {batch.batch_id}"
        ) from exc
    except ClientError as exc:
        raise RetryableSourceError(
            f"HTTP client error while fetching batch {batch.batch_id}: {exc}"
        ) from exc
    items = payload.get("items", [])
    extracted = [
        ExtractedRecord(
            record_id=str(item["id"]),
            payload={**item, "batch_id": payload.get("batch_id", batch.batch_id)},
        )
        for item in items
    ]
    await asyncio.sleep(random.randint(1,30)) # Inject a pause into the execution for demo purposes 

    activity.logger.info(
        "Fetched %s records from HTTP batch %s",
        len(extracted),
        batch.batch_id,
    )
    return ExtractBatchResult(
        batch=batch,
        records=extracted,
        attempt=activity.info().attempt,
    )


@activity.defn
async def fetch_object_batch(request: ObjectBatchRequest) -> ExtractBatchResult:
    """Read a batch of records from NDJSON files under object storage."""
    config = request.config
    batch = request.batch
    file_path = Path(config.root_path) / batch.batch_id
    if not file_path.exists():
        raise NonRetryableSourceError(f"Batch file {file_path} is missing")
    activity.logger.info("Reading batch file %s", file_path)
    extracted: List[ExtractedRecord] = []
    with file_path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            line = line.strip()
            if not line:
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError as exc:
                raise NonRetryableSourceError(
                    f"Invalid JSON on line {line_number} of {file_path}: {exc}"
                ) from exc
            extracted.append(
                ExtractedRecord(
                    record_id=str(payload["id"]),
                    payload={**payload, "batch_id": batch.batch_id},
                )
            )
    await asyncio.sleep(random.randint(0,35)) # Inject a pause into the execution for demo purposes 
    activity.logger.info(
        "Read %s records from file batch %s",
        len(extracted),
        batch.batch_id,
    )
    return ExtractBatchResult(
        batch=batch,
        records=extracted,
        attempt=activity.info().attempt,
    )


@activity.defn
async def transform_batch(request: TransformBatchRequest) -> TransformBatchResult:
    """Transform raw records into normalized JSON blobs."""
    transformed = normalize_records(request.records, request.config)
    activity.logger.info(
        "Transformed %s records for batch %s",
        len(transformed),
        request.batch.batch_id,
    )
    
    await asyncio.sleep(random.randint(0,25)) # Inject a pause into the execution for demo purposes 

    return TransformBatchResult(
        batch=request.batch,
        records=[record.model_dump() for record in transformed],
        attempt=activity.info().attempt,
    )


@activity.defn
async def load_batch(request: LoadBatchRequest) -> LoadBatchResult:
    """Load transformed records into the SQLite sink with heartbeats."""
    _ensure_sink(request.config)
    info = activity.info()
    resume_index = 0
    if info.heartbeat_details:
        resume_index = int(info.heartbeat_details[0].get("next_index", 0))
    records = request.records
    total = len(records)
    activity.logger.info(
        "Loading %s transformed records for batch %s (resuming from index %s)",
        total,
        request.batch.batch_id,
        resume_index,
    )
    inserted = 0
    skipped = 0
    path = Path(request.config.sqlite_path)
    with closing(sqlite3.connect(path)) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        for index in range(resume_index, total):
            record_dict = records[index]
            data = {
                "record_id": record_dict["record_id"],
                "run_id": request.run_id,
                "batch_id": request.batch.batch_id,
                "normalized": json.dumps(record_dict["normalized"]),
                "checksum": record_dict["checksum"],
            }
            try:
                before = conn.total_changes
                conn.execute(
                    f"""
                    INSERT INTO {request.config.table_name} (
                        record_id, run_id, batch_id, normalized, checksum, ingested_at
                    )
                    VALUES (:record_id, :run_id, :batch_id, :normalized, :checksum, datetime('now'))
                    ON CONFLICT(record_id) DO NOTHING
                    """,
                    data,
                )
                after = conn.total_changes
                if after > before:
                    inserted += 1
                else:
                    skipped += 1
                conn.commit()
            except sqlite3.OperationalError as exc:  # pragma: no cover - defensive
                raise SinkWriteError(f"Operational error writing to sink: {exc}") from exc
            except sqlite3.Error as exc:  # pragma: no cover - defensive
                raise SinkWriteError(
                    f"Unexpected SQLite error writing record {data['record_id']}: {exc}",
                    retryable=False,
                ) from exc
            # Heartbeat after each record to checkpoint progress for safe retries
            activity.heartbeat(
                {
                    "next_index": index + 1,
                    "record_id": data["record_id"],
                }
            )
    activity.logger.info(
        "Loaded %s records (inserted=%s skipped=%s) for batch %s",
        total,
        inserted,
        skipped,
        request.batch.batch_id,
    )

    await asyncio.sleep(random.randint(1,30)) # Inject a pause into the execution for demo purposes 

    return LoadBatchResult(
        inserted_count=inserted,
        skipped_count=skipped,
        attempt=activity.info().attempt,
    )
