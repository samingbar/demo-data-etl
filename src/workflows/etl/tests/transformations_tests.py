"""Unit tests for deterministic transformation helpers."""

from datetime import datetime, timezone

from workflows.etl.models import ExtractedRecord, TransformConfig
from workflows.etl.transformations import normalize_record


def test_normalize_record_outputs_consistent_structure() -> None:
    """Ensure normalization enriches records deterministically."""
    record = ExtractedRecord(
        record_id="abc-123",
        payload={
            "customer_id": "CUST-001",
            "amount": 19.987,
            "currency": "usd",
            "country": "us",
            "email": "User@Example.Com",
            "created_at": "2024-01-01T00:00:00Z",
            "status": "COMPLETED",
            "source_system": "http_api",
            "batch_id": "batch-1",
        },
    )
    config = TransformConfig(country_fallback="GB", normalize_email_domain=True)
    normalized = normalize_record(record, config)
    assert normalized.record_id == "abc-123"
    assert normalized.normalized["email"] == "user@example.com"
    assert normalized.normalized["country"] == "US"
    assert normalized.normalized["amount"] == 19.99
    assert normalized.normalized["status"] == "completed"
    assert normalized.source_batch_id == "batch-1"
    # Checksum should remain stable for identical inputs
    checksum_repeat = normalize_record(record, config).checksum
    assert normalized.checksum == checksum_repeat


def test_normalize_record_applies_country_fallback() -> None:
    """Missing country codes should fall back to configured value."""
    record = ExtractedRecord(
        record_id="def-456",
        payload={
            "customer_id": "CUST-002",
            "amount": 12,
            "currency": "eur",
            "email": "other@example.com",
            "created_at": datetime.now(tz=timezone.utc).isoformat(),
            "status": "pending",
            "source_system": "http_api",
            "batch_id": "batch-2",
        },
    )
    config = TransformConfig(country_fallback="NL", normalize_email_domain=False)
    normalized = normalize_record(record, config)
    assert normalized.normalized["country"] == "NL"
    assert normalized.normalized["email"] == "other@example.com"
