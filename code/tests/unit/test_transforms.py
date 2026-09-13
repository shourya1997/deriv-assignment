from datetime import date

import pytest

from deriv_pipeline.transforms import compute_late_arrival, earliest_op_per_client, resolve_header


def test_earliest_op_per_client_picks_lowest_lsn():
    records = [
        {"lsn": 1004, "client_id": "CL001", "op": "update"},
        {"lsn": 1001, "client_id": "CL030", "op": "insert"},
        {"lsn": 1012, "client_id": "CL002", "op": "update"},
    ]
    assert earliest_op_per_client(records) == {
        "CL001": "update", "CL030": "insert", "CL002": "update",
    }


def test_earliest_op_per_client_ignores_lsn_input_order():
    records = [
        {"lsn": 1010, "client_id": "CL001", "op": "delete"},
        {"lsn": 1004, "client_id": "CL001", "op": "update"},
    ]
    assert earliest_op_per_client(records) == {"CL001": "update"}

EXPECTED = [
    "deposit_id", "client_id", "deposit_date", "amount_usd", "payment_method",
    "currency_original", "exchange_rate", "status", "processing_days", "fee_usd",
]
ALIASES = {"method": "payment_method"}


def test_resolve_header_no_drift_on_expected_columns():
    result = resolve_header(EXPECTED, EXPECTED, ALIASES)
    assert result.colmap == {c: c for c in EXPECTED}
    assert result.schema_drift_detected is False
    assert result.unrecognized == []


def test_resolve_header_flags_drift_on_aliased_column():
    header = [c if c != "payment_method" else "method" for c in EXPECTED]
    result = resolve_header(header, EXPECTED, ALIASES)
    assert result.colmap["method"] == "payment_method"
    assert result.schema_drift_detected is True
    assert result.unrecognized == []


def test_resolve_header_flags_drift_and_drops_unrecognized_column():
    header = EXPECTED + ["totally_new_column"]
    result = resolve_header(header, EXPECTED, ALIASES)
    assert result.schema_drift_detected is True
    assert result.unrecognized == ["totally_new_column"]
    assert "totally_new_column" not in result.colmap


def test_compute_late_arrival_true_when_over_threshold():
    # filename delivery date 2024-03-03, event (deposit_date) 2024-02-26 -> 6 days > 2
    assert compute_late_arrival(
        "deposits_vendor_20240303.csv", date(2024, 2, 26),
        r"deposits_vendor_(\d{8})\.csv", 2,
    ) is True


def test_compute_late_arrival_false_within_threshold():
    assert compute_late_arrival(
        "deposits_vendor_20240301.csv", date(2024, 3, 1),
        r"deposits_vendor_(\d{8})\.csv", 2,
    ) is False


def test_compute_late_arrival_raises_if_pattern_does_not_match():
    with pytest.raises(ValueError, match="did not match"):
        compute_late_arrival(
            "unexpected_name.csv", date(2024, 3, 1),
            r"deposits_vendor_(\d{8})\.csv", 2,
        )
