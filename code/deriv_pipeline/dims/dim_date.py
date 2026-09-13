"""dim_date generation. `date_row()` is the pure part (unit-testable); shared
by the walking skeleton's on-demand `ensure_date()` here and, from Step 4
onward, the config-driven `generated_dimension` engine that pre-populates the
full 2024-01-01..2024-12-31 range up front — same row shape either way, so
Step 4 replacing the on-demand call site doesn't change what a date_key means."""
from __future__ import annotations

from datetime import date

_DAY_NAMES = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
_MONTH_NAMES = [
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
]


def date_row(d: date) -> dict:
    return {
        "date_key": int(d.strftime("%Y%m%d")),
        "full_date": d,
        "day_of_week": d.isoweekday(),
        "day_name": _DAY_NAMES[d.weekday()],
        "month": d.month,
        "month_name": _MONTH_NAMES[d.month - 1],
        "quarter": (d.month - 1) // 3 + 1,
        "year": d.year,
        "is_weekend": d.isoweekday() >= 6,
    }


def ensure_date(conn, d: date) -> int:
    """Idempotently ensures a warehouse.dim_date row exists for `d`; returns
    its date_key. Uses ON CONFLICT DO NOTHING so re-running is a no-op."""
    row = date_row(d)
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO warehouse.dim_date
                (date_key, full_date, day_of_week, day_name, month, month_name,
                 quarter, year, is_weekend)
            VALUES (%(date_key)s, %(full_date)s, %(day_of_week)s, %(day_name)s,
                    %(month)s, %(month_name)s, %(quarter)s, %(year)s, %(is_weekend)s)
            ON CONFLICT (date_key) DO NOTHING
            """,
            row,
        )
    return row["date_key"]
