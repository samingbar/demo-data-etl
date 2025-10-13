"""Deterministic transformation helpers used by the ETL workflow."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from typing import Any, Dict

from .models import ExtractedRecord, TransformConfig, TransformedRecord


def normalize_record(record: ExtractedRecord, config: TransformConfig) -> TransformedRecord:
    """Normalize and enrich a raw record into a warehouse-ready structure."""
    payload = record.payload
    email = payload.get("email", "")
    country = payload.get("country") or config.country_fallback
    amount = float(payload.get("amount", 0))
    currency = (payload.get("currency") or "USD").upper()
    processed_at = datetime.now(tz=timezone.utc).isoformat()
    normalized: Dict[str, Any] = {
        "record_id": record.record_id,
        "customer_id": payload.get("customer_id"),
        "country": country.upper(),
        "email": email.lower() if config.normalize_email_domain else email,
        "amount": round(amount, 2),
        "currency": currency,
        "source_created_at": payload.get("created_at"),
        "processed_at": processed_at,
        "status": payload.get("status", "unknown").lower(),
        "source_system": payload.get("source_system", "http_api"),
    }
    checksum = hashlib.sha256(str(sorted(normalized.items())).encode("utf-8")).hexdigest()
    return TransformedRecord(
        record_id=record.record_id,
        normalized=normalized,
        checksum=checksum,
        source_batch_id=payload.get("batch_id", "unknown"),
    )


def normalize_records(records: list[ExtractedRecord], config: TransformConfig) -> list[TransformedRecord]:
    """Normalize a collection of records."""
    return [normalize_record(record, config) for record in records]

