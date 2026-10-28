from collections.abc import Mapping, Sequence

def validate_schema(
    actual_schema: Mapping[str, str],
    expected_schema: Mapping[str, str],
    *,
    allow_extra_columns: bool = False,
) -> dict:
    """Validate column names and data types.

    Example:
        validate_schema(
            {"order_id": "string", "quantity": "int"},
            {"order_id": "string", "quantity": "int"},
        )
    """
    missing = sorted(set(expected_schema) - set(actual_schema))
    extra = sorted(set(actual_schema) - set(expected_schema))
    type_mismatches = {
        name: {"expected": expected_schema[name], "actual": actual_schema[name]}
        for name in expected_schema.keys() & actual_schema.keys()
        if str(expected_schema[name]).lower() != str(actual_schema[name]).lower()
    }

    errors = []
    if missing:
        errors.append(f"Missing columns: {missing}")
    if extra and not allow_extra_columns:
        errors.append(f"Unexpected columns: {extra}")
    if type_mismatches:
        errors.append(f"Data type mismatches: {type_mismatches}")

    result = {
        "valid": not errors,
        "missing_columns": missing,
        "extra_columns": extra,
        "type_mismatches": type_mismatches,
        "errors": errors,
    }
    if errors:
        raise ValueError("; ".join(errors))
    return result
