from datetime import datetime
from typing import Any

def get_incremental_watermark(
    last_successful_watermark: Any = None,
    *,
    fallback: Any = None,
) -> Any:
    """Return the last successful watermark, or fallback for the first run.

    The function does not query a database. The caller retrieves the persisted
    watermark from its metadata store and passes it in.
    """
    watermark = last_successful_watermark
    if watermark is None:
        watermark = fallback
    if watermark is None:
        raise ValueError("No watermark available: provide a saved watermark or fallback")
    return watermark
