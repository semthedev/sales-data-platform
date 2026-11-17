"""Row-level comparison of two independent implementations of the store performance mart."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal

MART_KEY = "plant"
TEXT_COLUMNS = ("store_name",)
NUMERIC_COLUMNS = (
    "gross_revenue",
    "discount",
    "net_revenue",
    "sold_quantity",
    "bills_count",
    "traffic",
    "promo_sold",
    "promo_rate_pct",
    "avg_items_per_bill",
    "conversion_rate_pct",
    "avg_bill_amount",
    "revenue_per_visitor",
)
DEFAULT_TOLERANCE = Decimal("0.01")

Row = Mapping[str, object]


class MartParityError(RuntimeError):
    """Raised when the Spark mart does not match the SQL mart."""


@dataclass(frozen=True, slots=True)
class Difference:
    message: str
    store: str | None = None
    column: str | None = None


def _as_decimal(value: object) -> Decimal | None:
    return None if value is None else Decimal(str(value))


def _index_by_key(rows: Sequence[Row], side: str, differences: list[Difference]) -> dict[str, Row]:
    indexed: dict[str, Row] = {}
    for row in rows:
        key = str(row[MART_KEY])
        if key in indexed:
            differences.append(Difference(f"duplicate {MART_KEY} in {side} mart", store=key))
        indexed[key] = row
    return indexed


def _compare_row(
    key: str, expected: Row, actual: Row, tolerance: Decimal, names: tuple[str, str]
) -> list[Difference]:
    expected_name, actual_name = names
    differences: list[Difference] = []
    for column in TEXT_COLUMNS:
        if expected[column] != actual[column]:
            differences.append(
                Difference(
                    f"{expected_name}={expected[column]!r}, {actual_name}={actual[column]!r}",
                    store=key,
                    column=column,
                )
            )
    for column in NUMERIC_COLUMNS:
        expected_value = _as_decimal(expected[column])
        actual_value = _as_decimal(actual[column])
        if expected_value is None or actual_value is None:
            matches = expected_value == actual_value
            detail = f"{expected_name}={expected_value}, {actual_name}={actual_value}"
        else:
            difference = abs(expected_value - actual_value)
            matches = difference <= tolerance
            detail = (
                f"{expected_name}={expected_value}, {actual_name}={actual_value}, diff={difference}"
            )
        if not matches:
            differences.append(Difference(detail, store=key, column=column))
    return differences


def compare_marts(
    expected: Sequence[Row],
    actual: Sequence[Row],
    tolerance: Decimal = DEFAULT_TOLERANCE,
    names: tuple[str, str] = ("sql", "spark"),
) -> list[Difference]:
    """Compare two marts row by row; an empty result means the marts match."""
    expected_name, actual_name = names
    differences: list[Difference] = []
    if len(expected) != len(actual):
        differences.append(
            Difference(
                f"row count differs: {expected_name}={len(expected)}, {actual_name}={len(actual)}"
            )
        )

    expected_rows = _index_by_key(expected, expected_name, differences)
    actual_rows = _index_by_key(actual, actual_name, differences)
    for key in sorted(expected_rows.keys() - actual_rows.keys()):
        differences.append(Difference(f"store missing from {actual_name} mart", store=key))
    for key in sorted(actual_rows.keys() - expected_rows.keys()):
        differences.append(Difference(f"store missing from {expected_name} mart", store=key))

    for key in sorted(expected_rows.keys() & actual_rows.keys()):
        differences.extend(
            _compare_row(key, expected_rows[key], actual_rows[key], tolerance, names)
        )
    return differences


def format_differences(differences: Sequence[Difference]) -> str:
    stores = sorted({item.store for item in differences if item.store})
    columns = sorted({item.column for item in differences if item.column})
    lines = [f"Spark mart does not match SQL mart: {len(differences)} differences"]
    if stores:
        lines.append(f"stores: {', '.join(stores)}")
    if columns:
        lines.append(f"columns: {', '.join(columns)}")
    for item in differences:
        location = ".".join(part for part in (item.store, item.column) if part)
        lines.append(f"  - {location}: {item.message}" if location else f"  - {item.message}")
    return "\n".join(lines)


def check_parity(
    expected: Sequence[Row],
    actual: Sequence[Row],
    tolerance: Decimal = DEFAULT_TOLERANCE,
) -> None:
    """Raise MartParityError naming the stores and columns that differ."""
    differences = compare_marts(expected, actual, tolerance)
    if differences:
        raise MartParityError(format_differences(differences))
