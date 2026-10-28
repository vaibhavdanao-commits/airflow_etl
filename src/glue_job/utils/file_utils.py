from pathlib import Path

def validate_file(
    file_path: str | Path,
    *,
    allowed_extensions: tuple[str, ...] = (".csv", ".parquet", ".json"),
    min_size_bytes: int = 1,
) -> dict:
    """Check that a source file exists, is a regular file, has an allowed extension,
    and meets the minimum size requirement.
    """
    path = Path(file_path)
    errors = []

    if not path.exists():
        errors.append(f"File does not exist: {path}")
    elif not path.is_file():
        errors.append(f"Path is not a file: {path}")
    else:
        if path.suffix.lower() not in {ext.lower() for ext in allowed_extensions}:
            errors.append(f"Unsupported extension '{path.suffix}'. Allowed: {allowed_extensions}")
        size = path.stat().st_size
        if size < min_size_bytes:
            errors.append(f"File size {size} bytes is below minimum {min_size_bytes}")

    result = {
        "valid": not errors,
        "file_name": path.name,
        "file_path": str(path),
        "file_size_bytes": path.stat().st_size if path.exists() and path.is_file() else 0,
        "errors": errors,
    }
    if errors:
        raise ValueError("; ".join(errors))
    return result
