from datetime import date

from deriv_pipeline.dims.dim_date import date_row


def test_date_row_shape_for_a_weekday():
    row = date_row(date(2024, 3, 1))  # Friday
    assert row["date_key"] == 20240301
    assert row["day_name"] == "Friday"
    assert row["month_name"] == "March"
    assert row["quarter"] == 1
    assert row["year"] == 2024
    assert row["is_weekend"] is False


def test_date_row_flags_weekend():
    row = date_row(date(2024, 3, 3))  # Sunday
    assert row["is_weekend"] is True
