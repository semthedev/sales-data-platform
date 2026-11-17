from __future__ import annotations

from decimal import Decimal

import pytest

from sales_pipeline.parity import NUMERIC_COLUMNS, MartParityError, check_parity, compare_marts


def make_row(plant: str, **overrides: object) -> dict[str, object]:
    row: dict[str, object] = {"plant": plant, "store_name": f"Store {plant}"}
    row.update({column: Decimal("10.00") for column in NUMERIC_COLUMNS})
    row.update(overrides)
    return row


def test_identical_marts_have_no_differences() -> None:
    rows = [make_row("M001"), make_row("M002")]
    assert compare_marts(rows, [dict(row) for row in rows]) == []


def test_differences_within_tolerance_are_accepted() -> None:
    expected = [make_row("M001", discount=Decimal("10.00"))]
    actual = [make_row("M001", discount=Decimal("10.01"))]
    assert compare_marts(expected, actual) == []


def test_reports_store_and_column_of_a_mismatch() -> None:
    expected = [make_row("M001"), make_row("M002")]
    actual = [make_row("M001"), make_row("M002", net_revenue=Decimal("10.50"))]

    with pytest.raises(MartParityError) as error:
        check_parity(expected, actual)

    message = str(error.value)
    assert "stores: M002" in message
    assert "columns: net_revenue" in message
    assert "sql=10.00, spark=10.50" in message


def test_reports_missing_and_extra_stores() -> None:
    differences = compare_marts([make_row("M001"), make_row("M002")], [make_row("M001")])
    messages = [(item.store, item.message) for item in differences]

    assert ("M002", "store missing from spark mart") in messages
    assert any("row count differs" in message for _, message in messages)


def test_null_against_value_is_a_mismatch() -> None:
    differences = compare_marts([make_row("M001")], [make_row("M001", traffic=None)])
    assert [(item.store, item.column) for item in differences] == [("M001", "traffic")]
