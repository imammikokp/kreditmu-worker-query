from typing import Any, Tuple


def _read_attr(obj: Any, names: Tuple[str, ...]) -> Any:
    """
    Safely read the first available attribute from `obj` among `names`.
    Useful to support both styles like `tenor1_*` and `tenor_1_*`.
    """
    for n in names:
        if hasattr(obj, n):
            return getattr(obj, n)
    return None


def _to_float(value: Any) -> float:
    """
    Convert value to float, defaulting None or invalid values to 0.0.
    """
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def apply_limit_snapshot_no_null(customer: Any, limit: Any):
    """
    Copy gross and remaining limit values from `customer` to `limit` while
    coercing None to 0.0, ensuring no nulls in the target object.

    Returns a tuple (pointer_false, limit) to match the requested signature.
    """
    pointer_false = False  # setara dengan `pointerFalse := false`

    # calculate current limit dan update
    # Jika perlu mapping lain, tambahkan sesuai kebutuhan Anda
    # customer_mapped = to_customer_response(customer)

    limit.tenor1_gross_limit_amount = _to_float(
        _read_attr(customer, ("tenor1_gross_limit_amount", "tenor_1_gross_limit_amount"))
    )
    limit.tenor1_remaining_limit = _to_float(
        _read_attr(customer, ("tenor1_remaining_limit", "tenor_1_remaining_limit"))
    )
    limit.tenor3_gross_limit_amount = _to_float(
        _read_attr(customer, ("tenor3_gross_limit_amount", "tenor_3_gross_limit_amount"))
    )
    limit.tenor3_remaining_limit = _to_float(
        _read_attr(customer, ("tenor3_remaining_limit", "tenor_3_remaining_limit"))
    )
    limit.tenor6_gross_limit_amount = _to_float(
        _read_attr(customer, ("tenor6_gross_limit_amount", "tenor_6_gross_limit_amount"))
    )
    limit.tenor6_remaining_limit = _to_float(
        _read_attr(customer, ("tenor6_remaining_limit", "tenor_6_remaining_limit"))
    )
    limit.tenor12_gross_limit_amount = _to_float(
        _read_attr(customer, ("tenor12_gross_limit_amount", "tenor_12_gross_limit_amount"))
    )
    limit.tenor12_remaining_limit = _to_float(
        _read_attr(customer, ("tenor12_remaining_limit", "tenor_12_remaining_limit"))
    )

    return pointer_false, limit