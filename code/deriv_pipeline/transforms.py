"""Pure, DB-free transforms shared by layer1/layer2 (see layers/). Kept
separate and dependency-free so unit tests exercise them against hand-written
fixture configs, never shipped ones — editing a real config must never break
this suite for unrelated reasons."""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class ResolvedHeader:
    # observed column name -> canonical (post-alias) column name, for every
    # observed column that resolves to something in expected_columns.
    colmap: dict[str, str]
    # true if any observed column needed the alias map, or any observed
    # column isn't recognized at all (expected ∪ aliases.keys) — both are
    # "the vendor's contract changed since we last looked" (part1_pipeline.md
    # §"Vendor schema drift").
    schema_drift_detected: bool
    # observed columns that resolve to nothing recognized; dropped, not
    # loaded into staging, but still count as drift above.
    unrecognized: list[str]


def resolve_header(
    header: list[str], expected_columns: list[str], aliases: dict[str, str]
) -> ResolvedHeader:
    expected = set(expected_columns)
    colmap: dict[str, str] = {}
    drift = False
    unrecognized: list[str] = []
    for observed in header:
        if observed in expected:
            colmap[observed] = observed
        elif observed in aliases:
            colmap[observed] = aliases[observed]
            drift = True
        else:
            unrecognized.append(observed)
            drift = True
    return ResolvedHeader(colmap=colmap, schema_drift_detected=drift, unrecognized=unrecognized)


def compute_late_arrival(
    source_file: str, event_date: date, pattern: str, threshold_days: int
) -> bool:
    """(file_delivery_date - event_date) > threshold_days, per
    part1_pipeline.md §3.1. `pattern` must have exactly one capture group,
    an 8-digit YYYYMMDD, per the config's `late_arrival.delivery_date`
    block."""
    match = re.search(pattern, source_file)
    if not match:
        raise ValueError(
            f"late_arrival pattern {pattern!r} did not match source_file {source_file!r}"
        )
    raw = match.group(1)
    delivery_date = date(int(raw[0:4]), int(raw[4:6]), int(raw[6:8]))
    return (delivery_date - event_date).days > threshold_days
