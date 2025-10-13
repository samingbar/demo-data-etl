"""Pydantic models and enums used by the ETL workflow demo."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl, validator


class SourceType(str, Enum):
    """Supported data sources for the ETL demo."""

    HTTP_API = "http_api"
    OBJECT_STORAGE = "object_storage"


class HttpSourceConfig(BaseModel):
    """Configuration for the synthetic HTTP API source."""

    base_url: HttpUrl = Field(
        ...,
        description="Base URL for the mock API (e.g. http://localhost:8081).",
    )
    dataset: str = Field(
        default="transactions",
        description="Dataset segment to request from the API.",
    )
    page_size: int = Field(
        default=50,
        ge=1,
        le=500,
        description="Page size to request from the API.",
    )
    max_pages: int = Field(
        default=5,
        ge=1,
        description="Maximum number of pages to ingest for the demo run.",
    )
    request_timeout_seconds: float = Field(
        default=5.0,
        gt=0,
        description="Timeout applied to HTTP requests.",
    )


class ObjectStorageSourceConfig(BaseModel):
    """Configuration for the local object storage batch files."""

    root_path: str = Field(
        default="data/source_batches",
        description="Local path that contains NDJSON batches.",
    )
    file_glob: str = Field(
        default="*.ndjson",
        description="Glob pattern used to enumerate candidate batch files.",
    )


class SinkConfig(BaseModel):
    """Configuration for the downstream warehouse sink."""

    sqlite_path: str = Field(
        default="data/warehouse/warehouse.db",
        description="SQLite file that acts as the customer-owned warehouse.",
    )
    table_name: str = Field(
        default="etl_records",
        description="Table name receiving transformed records.",
    )


class TransformConfig(BaseModel):
    """Configuration for deterministic enrichment logic."""

    country_fallback: str = Field(
        default="UNK",
        description="Fallback ISO country code when source data omits it.",
    )
    normalize_email_domain: bool = Field(
        default=True,
        description="Toggle to normalize email domains to lowercase.",
    )


class EtlWorkflowInput(BaseModel):
    """Top-level configuration passed to the workflow."""

    run_id: str = Field(
        ...,
        description="Stable identifier used for logging and idempotency scopes.",
    )
    source_type: SourceType = Field(
        default=SourceType.HTTP_API,
        description="Selected source for this ingestion run.",
    )
    http_source: Optional[HttpSourceConfig] = Field(
        default=None,
        description="Configuration for HTTP source; required when source_type=http_api.",
    )
    object_source: Optional[ObjectStorageSourceConfig] = Field(
        default=None,
        description="Configuration for object storage source; required when source_type=object_storage.",
    )
    sink: SinkConfig = Field(default_factory=SinkConfig)
    transform: TransformConfig = Field(default_factory=TransformConfig)
    max_concurrent_batches: int = Field(
        default=3,
        ge=1,
        le=20,
        description="Maximum number of batches processed in parallel.",
    )

    @validator("http_source", always=True)
    def validate_http_source(cls, value: Optional[HttpSourceConfig], values: Dict[str, Any]) -> Optional[HttpSourceConfig]:
        """Ensure HTTP source settings are present when required."""
        if values.get("source_type") is SourceType.HTTP_API and value is None:
            raise ValueError("http_source must be provided when source_type is http_api")
        return value

    @validator("object_source", always=True)
    def validate_object_source(
        cls,
        value: Optional[ObjectStorageSourceConfig],
        values: Dict[str, Any],
    ) -> Optional[ObjectStorageSourceConfig]:
        """Ensure object storage settings are present when required."""
        if values.get("source_type") is SourceType.OBJECT_STORAGE and value is None:
            raise ValueError("object_source must be provided when source_type is object_storage")
        return value


class SourceBatch(BaseModel):
    """Descriptor for a batch unit processed by the workflow."""

    batch_id: str
    sequence: int
    approx_item_count: int = 0


class ExtractedRecord(BaseModel):
    """Raw record obtained from the source."""

    record_id: str
    payload: Dict[str, Any]


class TransformedRecord(BaseModel):
    """Normalized and enriched record ready for loading."""

    record_id: str
    normalized: Dict[str, Any]
    checksum: str
    source_batch_id: str


class ProgressSnapshot(BaseModel):
    """Point-in-time progress details returned via workflow query."""

    run_id: str
    total_batches: int
    batches_completed: int
    total_items: int
    items_processed: int
    retry_count: int
    paused: bool
    last_error: Optional[str] = None
    last_updated: datetime


class ActivityRetryContext(BaseModel):
    """Details emitted when an activity retries."""

    activity: str
    attempt: int
    max_attempts: int
    reason: str
    batch_id: Optional[str] = None
    record_id: Optional[str] = None


class EnumerateBatchesInput(BaseModel):
    """Input for batch enumeration activity."""

    workflow_input: EtlWorkflowInput


class HttpBatchRequest(BaseModel):
    """Input for fetching a single HTTP API batch."""

    config: HttpSourceConfig
    batch: SourceBatch


class ObjectBatchRequest(BaseModel):
    """Input for fetching a batch from object storage."""

    config: ObjectStorageSourceConfig
    batch: SourceBatch


class TransformBatchRequest(BaseModel):
    """Input for transforming a batch of extracted records."""

    run_id: str
    config: TransformConfig
    batch: SourceBatch
    records: List[ExtractedRecord]


class LoadBatchRequest(BaseModel):
    """Input for writing transformed records to the sink."""

    run_id: str
    config: SinkConfig
    batch: SourceBatch
    records: List[Dict[str, Any]]


class LoadBatchResult(BaseModel):
    """Result returned from sink activity."""

    inserted_count: int
    skipped_count: int
    attempt: int


class ExtractBatchResult(BaseModel):
    """Result returned from extract activities."""

    batch: SourceBatch
    records: List[ExtractedRecord]
    attempt: int


class TransformBatchResult(BaseModel):
    """Result returned from transform activity."""

    batch: SourceBatch
    records: List[Dict[str, Any]]
    attempt: int
