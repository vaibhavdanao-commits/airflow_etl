import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

def log_ingestion(
    *,
    file_name: str,
    source: str,
    record_count: int,
    processing_status: str,
    processing_time_seconds: float,
    pipeline_version: str,
    error_count: int = 0,
    log_file: str | Path = "logs/ingestion_metadata.jsonl",
    extra: dict[str, Any] | None = None,
) -> dict:
    """Append one JSON Lines metadata record and also emit a standard log entry."""
    if record_count < 0 or error_count < 0 or processing_time_seconds < 0:
        raise ValueError("Counts and processing time must not be negative")
    status = processing_status.upper()
    if status not in {"SUCCESS", "FAILED", "PARTIAL"}:
        raise ValueError("processing_status must be SUCCESS, FAILED, or PARTIAL")

    record = {
        "file_name": file_name,
        "source": source,
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
        "record_count": record_count,
        "processing_status": status,
        "processing_time_seconds": round(float(processing_time_seconds), 3),
        "pipeline_version": pipeline_version,
        "error_count": error_count,
    }
    if extra:
        record["extra"] = extra

    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, default=str) + "\\n")

    logging.info("Ingestion metadata: %s", json.dumps(record, default=str))
    return record
