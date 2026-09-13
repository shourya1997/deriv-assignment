"""generated_dimension engine: pre-populates warehouse.dim_date for the
configured [from, to] range, reusing dim_date.ensure_date so the row shape
matches Step 3's on-demand backfill exactly (see dim_date.py's docstring)."""
from __future__ import annotations

from datetime import timedelta

from deriv_pipeline.config import GeneratedDimensionConfig
from deriv_pipeline.dims.dim_date import ensure_date


def load(cfg: GeneratedDimensionConfig, conn) -> int:
    d = cfg.from_date
    count = 0
    while d <= cfg.to_date:
        ensure_date(conn, d)
        d += timedelta(days=1)
        count += 1
    return count
