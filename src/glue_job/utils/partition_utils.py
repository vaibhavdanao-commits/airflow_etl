from datetime import date, datetime
from pathlib import PurePosixPath

def generate_partition_path(
    base_path: str,
    partition_date: date | datetime | str,
    *,
    region: str | None = None,
    category: str | None = None,
) -> str:
    """Build a Hive-style partition path, e.g. curated/sales/year=2026/month=10/day=28.

    String dates must use YYYY-MM-DD. Optional partition values are sanitized
    to avoid introducing path separators.
    """
    if isinstance(partition_date, str):
        partition_date = date.fromisoformat(partition_date)
    if isinstance(partition_date, datetime):
        partition_date = partition_date.date()
    if not isinstance(partition_date, date):
        raise TypeError("partition_date must be date, datetime, or YYYY-MM-DD string")

    parts = [
        str(base_path).replace("\\\\", "/").rstrip("/"),
        f"year={partition_date:%Y}",
        f"month={partition_date:%m}",
        f"day={partition_date:%d}",
    ]
    for key, value in (("region", region), ("category", category)):
        if value is not None:
            safe = str(value).strip().replace("/", "_").replace("\\\\", "_")
            if not safe or safe in {".", ".."}:
                raise ValueError(f"Invalid {key} partition value")
            parts.append(f"{key}={safe}")
    return str(PurePosixPath(*parts))
