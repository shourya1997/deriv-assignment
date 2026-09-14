"""Config schema: one declarative YAML per table/dimension/reconciliation,
consumed identically by the layer engine, the DAG factory, and the test
suite (see ARCHITECTURE_DECISIONS.md ADR-1). `--validate-all` (the
__main__ block) is what airflow-init runs before the scheduler starts.

Every raise in this module includes the source file's path — this is the
only diagnostic signal available from airflow-init's one-shot container log
on a broken deploy (Step 2 dual review finding)."""
from __future__ import annotations

import datetime
import os
import sys
import traceback
from dataclasses import dataclass, field
from pathlib import Path

import yaml

# Deliberately not imported from deriv_pipeline.db: that module imports
# psycopg at module scope, which would make a pure-YAML validation pass
# (e.g. a config-only lint tool) needlessly depend on the DB driver (Step 2
# dual review finding).
REPO_ROOT = Path(__file__).resolve().parents[2]

# Bind-mounted at /opt/deriv/config inside every container (same pattern as
# sql/ and data/) so editing a YAML takes effect without an image rebuild;
# falls back to the repo-relative path for host/local-venv test runs. An
# explicitly empty env var is treated as unset, not as "./" (Step 2 review).
CONFIG_DIR = Path(os.environ.get("DERIV_CONFIG_DIR") or (REPO_ROOT / "code" / "config"))

LAYER3_STRATEGIES = {"dimension_upsert", "fact_upsert", "scd2_apply", "scd2_baseline_seed"}
TABLE_CONFIG_KINDS = {"table", "derived_dimension", "generated_dimension"}
DQ_SEVERITIES = {"INFO", "WARNING", "CRITICAL"}

# fact_upsert's fk_resolution[col] is either one of these two special
# strings, or a dict (validated below) shaped like dimension_upsert's
# fk_resolution for a plain dimension lookup.
_FACT_UPSERT_FK_SENTINELS = {"inferred_member_on_miss", "snapshotted_fk"}
_FK_RULE_REQUIRED_KEYS = {"from_column", "dim_target", "dim_natural_key", "dim_surrogate_key"}


def _load_yaml(path: Path) -> dict:
    raw = yaml.safe_load(Path(path).read_text())
    if not isinstance(raw, dict):
        raise ValueError(f"{path}: config file is empty or not a YAML mapping")
    return raw


def _require_str_list(raw: dict, key: str, path: Path, required: bool) -> list[str]:
    """Every list-of-column-name field (natural_key, expected_columns,
    update_columns, layer3[].columns) must actually be a YAML list, not a
    scalar — `natural_key: deposit_id` (missing brackets) is truthy and
    non-empty, so a bare `if not value` check silently accepts it and later
    iterates the string's characters instead of one column name (Step 2
    dual review finding, confirmed by both reviewers independently)."""
    value = raw.get(key)
    if value is None:
        if required:
            raise ValueError(f"{path}: {key} is required and must be a non-empty list")
        return []
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ValueError(f"{path}: {key} must be a YAML list of strings, got {value!r}")
    if required and not value:
        raise ValueError(f"{path}: {key} is required and must be a non-empty list")
    return value


@dataclass
class SourceConfig:
    format: str
    glob: str
    natural_key: list[str]
    expected_columns: list[str]
    aliases: dict[str, str] = field(default_factory=dict)
    late_arrival: dict | None = None


@dataclass
class DqCheck:
    """One config-declared SQL assertion (Step 8): `sql` must return a single
    row/column giving the number of rows that fail the check. Routed through
    the same severity levels as quarantine.rejected_rows (INFO/WARNING/
    CRITICAL) — a CRITICAL failure also quarantines a summary row; anything
    else is just logged to data_quality.dq_check_results for
    dq_table_health's regression tracking."""

    name: str
    sql: str
    severity: str


@dataclass
class LayerTarget:
    target: str
    conflict_strategy: str | None = None
    update_columns: list[str] = field(default_factory=list)
    ge_suite: str | None = None
    dq_checks: list[DqCheck] = field(default_factory=list)


@dataclass
class Layer3Target:
    target: str
    strategy: str
    fk_resolution: dict = field(default_factory=dict)
    columns: list[str] | None = None
    # scd2_baseline_seed only: glob (under data/) for the raw CDC source file
    # used to exclude clients whose earliest event is an 'insert' (ADR-2/G2).
    cdc_source_glob: str | None = None
    # fact_upsert only: the staging column holding this fact's event date —
    # used both for dim_date resolution and as the risk-snapshot lookup
    # timestamp (Step 5: generalized off vendor_deposits' original hardcoded
    # deposit_date so client_deposit/client_trades can reuse the same function).
    event_date_column: str | None = None
    # fact_upsert only: target_column -> literal value written on every row
    # (e.g. source_system) rather than copied from staging.
    literals: dict[str, str] = field(default_factory=dict)


def _build_dq_checks(raw: dict, path: Path, label: str) -> list[DqCheck]:
    """`dq.py` (Step 8) only ever reads `cfg.layer2.dq_checks` — a `dq_checks`
    block anywhere else would parse fine but silently run zero checks, so
    it's rejected here rather than left as a footgun."""
    raw_checks = raw.get("dq_checks")
    if raw_checks is None:
        return []
    if label != "layer2":
        raise ValueError(f"{path}: dq_checks is only read from layer2, found under {label}")
    if not isinstance(raw_checks, list):
        raise ValueError(f"{path}: dq_checks must be a YAML list, got {raw_checks!r}")

    checks = []
    seen_names = set()
    for entry in raw_checks:
        if not isinstance(entry, dict):
            raise ValueError(f"{path}: dq_checks entry must be a YAML mapping, got {entry!r}")
        severity = entry.get("severity")
        if severity not in DQ_SEVERITIES:
            raise ValueError(
                f"{path}: dq_checks severity {severity!r} must be one of {sorted(DQ_SEVERITIES)}"
            )
        name, sql = entry.get("name"), entry.get("sql")
        if not isinstance(name, str) or not isinstance(sql, str):
            raise ValueError(f"{path}: dq_checks entry requires string name and sql: {entry!r}")
        if name in seen_names:
            raise ValueError(f"{path}: duplicate dq_checks name {name!r}")
        seen_names.add(name)
        checks.append(DqCheck(name=name, sql=sql, severity=severity))
    return checks


def _build_layer_target(raw: dict, path: Path, label: str) -> LayerTarget:
    try:
        update_columns = _require_str_list(raw, "update_columns", path, required=False)
        return LayerTarget(
            target=raw["target"],
            conflict_strategy=raw.get("conflict_strategy"),
            update_columns=update_columns,
            ge_suite=raw.get("ge_suite"),
            dq_checks=_build_dq_checks(raw, path, label),
        )
    except (TypeError, KeyError) as exc:
        raise ValueError(f"{path}: invalid {label} block: {exc}") from exc


@dataclass
class TableConfig:
    kind: str
    name: str
    source: SourceConfig
    layer1: LayerTarget
    layer2: LayerTarget
    layer3: list[Layer3Target]
    orchestration: dict

    @classmethod
    def load(cls, path: Path) -> "TableConfig":
        path = Path(path)
        raw = _load_yaml(path)

        if "source" not in raw:
            raise ValueError(f"{path}: missing required top-level key 'source'")
        source_raw = dict(raw["source"])
        source = SourceConfig(
            format=source_raw.get("format", ""),
            glob=source_raw.get("glob", ""),
            natural_key=_require_str_list(source_raw, "natural_key", path, required=True),
            expected_columns=_require_str_list(
                source_raw, "expected_columns", path, required=False
            ),
            aliases=source_raw.get("aliases", {}),
            late_arrival=source_raw.get("late_arrival"),
        )

        for required_key in ("layer1", "layer2", "layer3", "kind", "name"):
            if required_key not in raw:
                raise ValueError(f"{path}: missing required top-level key {required_key!r}")

        layer1 = _build_layer_target(raw["layer1"], path, "layer1")
        layer2 = _build_layer_target(raw["layer2"], path, "layer2")

        layer3 = []
        for entry in raw["layer3"]:
            strategy = entry.get("strategy")
            if strategy not in LAYER3_STRATEGIES:
                raise ValueError(
                    f"{path}: unknown layer3 strategy {strategy!r};"
                    f" must be one of {sorted(LAYER3_STRATEGIES)}"
                )
            if strategy == "scd2_apply":
                # scd2_apply's Python side (layer3_warehouse.scd2_apply) calls
                # warehouse.apply_cdc_event() with a hardcoded column list
                # (client_id, lsn, commit_ts, op, after) and a hardcoded
                # target table — previously neither was checked against the
                # config, so a typo'd/incomplete expected_columns silently
                # NULLs the dimension on every apply (no exception, no
                # quarantine row, watermark still advances), and a
                # misconfigured `target` was simply ignored. Fail loud here
                # instead (Step 6 dual review finding, Opus).
                _REQUIRED = {"client_id", "lsn", "commit_ts", "op", "after"}
                missing = _REQUIRED - set(entry.get("columns") or []) - set(source.expected_columns)
                if missing:
                    raise ValueError(
                        f"{path}: layer3 strategy scd2_apply requires source.expected_columns"
                        f" to include {sorted(_REQUIRED)}; missing {sorted(missing)}"
                    )
                if entry.get("target") != "warehouse.dim_client_risk_snapshot":
                    raise ValueError(
                        f"{path}: layer3 strategy scd2_apply always writes to"
                        f" warehouse.dim_client_risk_snapshot (via apply_cdc_event) —"
                        f" target={entry.get('target')!r} would be silently ignored"
                    )
                if not raw.get("orchestration", {}).get("requires_scd2_baseline"):
                    # Without this, a typo'd/omitted requires_scd2_baseline
                    # silently drops the wait_for_scd2_baseline sensor and
                    # reintroduces the ADR-2 baseline-ordering race with no
                    # test catching it (test_dag_factory.py only asserts the
                    # sensor exists when the key is spelled right).
                    raise ValueError(
                        f"{path}: layer3 strategy scd2_apply requires"
                        f" orchestration.requires_scd2_baseline: true"
                    )
            if strategy == "scd2_baseline_seed" and not entry.get("cdc_source_glob"):
                # Without this, a misspelled/omitted key silently seeds a
                # fabricated baseline for an insert-first client (e.g. CL030)
                # instead of excluding it — exactly what ADR-2 says must
                # never happen. Fail loud at config-load time, not silently
                # at runtime (Step 4 dual review finding).
                raise ValueError(
                    f"{path}: layer3 strategy scd2_baseline_seed requires cdc_source_glob"
                )
            if strategy == "fact_upsert":
                # Without event_date_column, fact_upsert puts a bare None into
                # its generated column list and blows up with a confusing
                # TypeError deep in layer3 at DAG runtime instead of a clear,
                # path-annotated error at config-load time (Step 5 dual
                # review finding, confirmed independently by both reviewers).
                if not entry.get("event_date_column"):
                    raise ValueError(
                        f"{path}: layer3 strategy fact_upsert requires event_date_column"
                    )
                literals = entry.get("literals", {})
                if not isinstance(literals, dict):
                    raise ValueError(
                        f"{path}: layer3 literals must be a YAML mapping, got {literals!r}"
                    )
                for fk_col, rule in entry.get("fk_resolution", {}).items():
                    if isinstance(rule, dict):
                        missing = _FK_RULE_REQUIRED_KEYS - rule.keys()
                        if missing:
                            raise ValueError(
                                f"{path}: fk_resolution[{fk_col!r}] missing required"
                                f" key(s) {sorted(missing)}"
                            )
                    elif rule not in _FACT_UPSERT_FK_SENTINELS:
                        # A typo'd sentinel (e.g. "inferred_member" instead of
                        # "inferred_member_on_miss") silently flips behavior
                        # rather than erroring — must be rejected as loudly
                        # as a missing key (Step 5 dual review finding, Opus).
                        raise ValueError(
                            f"{path}: fk_resolution[{fk_col!r}]={rule!r} must be a dict or"
                            f" one of {sorted(_FACT_UPSERT_FK_SENTINELS)}"
                        )
            try:
                columns = _require_str_list(entry, "columns", path, required=False) or None
                layer3.append(
                    Layer3Target(
                        target=entry["target"],
                        strategy=strategy,
                        fk_resolution=entry.get("fk_resolution", {}),
                        columns=columns,
                        cdc_source_glob=entry.get("cdc_source_glob"),
                        event_date_column=entry.get("event_date_column"),
                        literals=entry.get("literals", {}),
                    )
                )
            except (TypeError, KeyError) as exc:
                raise ValueError(f"{path}: invalid layer3 entry: {exc}") from exc

        return cls(
            kind=raw["kind"],
            name=raw["name"],
            source=source,
            layer1=layer1,
            layer2=layer2,
            layer3=layer3,
            orchestration=raw.get("orchestration", {}),
        )

    @classmethod
    def load_all(cls, kind: str = "table") -> list["TableConfig"]:
        return [cfg for cfg in _load_all_table_dir() if isinstance(cfg, cls) and cfg.kind == kind]


@dataclass
class DerivedDimensionConfig:
    kind: str
    name: str
    source_column: str
    target: str
    target_key_column: str
    source_table: str | None = None
    # Set instead of source_table when the upstream table isn't onboarded yet
    # (e.g. dim_instrument derives from client_trades.json, but client_trades
    # isn't a `kind: table` config until Step 5) — reads distinct values
    # directly from the raw data/ file instead of a staged table.
    raw_source_glob: str | None = None
    # Optional column_name -> {natural_key_value: derived_value} maps, e.g.
    # dim_instrument's asset_class derived from the instrument name itself.
    derived_columns: dict[str, dict[str, str]] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> "DerivedDimensionConfig":
        path = Path(path)
        raw = _load_yaml(path)
        source_table = raw.get("source_table")
        raw_source_glob = raw.get("raw_source_glob")
        if not source_table and not raw_source_glob:
            raise ValueError(f"{path}: one of source_table or raw_source_glob is required")
        if source_table and raw_source_glob:
            raise ValueError(f"{path}: source_table and raw_source_glob are mutually exclusive")
        try:
            return cls(
                kind=raw["kind"],
                name=raw["name"],
                source_column=raw["source_column"],
                target=raw["target"],
                target_key_column=raw["target_key_column"],
                source_table=source_table,
                raw_source_glob=raw_source_glob,
                derived_columns=raw.get("derived_columns", {}),
            )
        except KeyError as exc:
            raise ValueError(f"{path}: missing required key {exc}") from exc

    @classmethod
    def load_all(cls) -> list["DerivedDimensionConfig"]:
        return [cfg for cfg in _load_all_table_dir() if isinstance(cfg, cls)]


@dataclass
class GeneratedDimensionConfig:
    kind: str
    name: str
    target: str
    from_date: datetime.date
    to_date: datetime.date

    @classmethod
    def load(cls, path: Path) -> "GeneratedDimensionConfig":
        path = Path(path)
        raw = _load_yaml(path)
        try:
            from_date = datetime.date.fromisoformat(str(raw["from"]))
            to_date = datetime.date.fromisoformat(str(raw["to"]))
        except (KeyError, ValueError) as exc:
            raise ValueError(f"{path}: invalid from/to date: {exc}") from exc
        if from_date > to_date:
            raise ValueError(f"{path}: from ({from_date}) is after to ({to_date})")
        try:
            return cls(
                kind=raw["kind"],
                name=raw["name"],
                target=raw["target"],
                from_date=from_date,
                to_date=to_date,
            )
        except KeyError as exc:
            raise ValueError(f"{path}: missing required key {exc}") from exc

    @classmethod
    def load_all(cls) -> list["GeneratedDimensionConfig"]:
        return [cfg for cfg in _load_all_table_dir() if isinstance(cfg, cls)]


_KIND_TO_CLASS = {
    "table": TableConfig,
    "derived_dimension": DerivedDimensionConfig,
    "generated_dimension": GeneratedDimensionConfig,
}


def _load_all_table_dir() -> list[object]:
    """Single enumerate-and-dispatch pass over config/tables/*.yml: reads
    each file's `kind` once (not once per loader, per Step 2 dual review's
    double-parse finding) and raises on any file whose `kind` isn't one of
    the three known table-dir kinds — an unrecognized/typo'd/missing `kind`
    used to be silently skipped by every loader, so a broken config file
    produced a green `--validate-all` and a silently-missing DAG (Step 2
    dual review finding)."""
    tables_dir = CONFIG_DIR / "tables"
    if not tables_dir.is_dir():
        raise ValueError(f"{tables_dir}: config/tables directory does not exist")
    results: list[object] = []
    for path in sorted(tables_dir.glob("*.yml")):
        raw = _load_yaml(path)
        kind = raw.get("kind")
        if kind not in TABLE_CONFIG_KINDS:
            raise ValueError(
                f"{path}: unknown kind {kind!r}; must be one of {sorted(TABLE_CONFIG_KINDS)}"
            )
        results.append(_KIND_TO_CLASS[kind].load(path))
    return results


@dataclass
class ReconciliationConfig:
    """Step 9: `left`/`right` are each a complete SQL SELECT returning the
    `key` columns, in that order, for one source's rows (e.g. vendor-feed vs.
    internal-portal deposits, both living in `warehouse.fact_deposits` but
    needing a join out to `dim_client` for `client_id` — per
    part1_pipeline.md section 3). Declaring full queries (not just a table +
    predicate) reuses Step 8's dq_checks pattern of config-declared SQL
    rather than teaching the engine to build joins generically for a design
    with exactly one real instance. A key tuple returned by one side and not
    the other is a discrepancy — nothing beyond tuple presence is compared,
    since the key itself (client_id, deposit_date, amount_usd) already
    carries every value that has to agree."""

    name: str
    key: list[str]
    left: str
    right: str

    @classmethod
    def load(cls, path: Path) -> "ReconciliationConfig":
        path = Path(path)
        raw = _load_yaml(path)
        try:
            key = _require_str_list(raw, "key", path, required=True)
            left, right = raw["left"], raw["right"]
        except KeyError as exc:
            raise ValueError(f"{path}: missing required key {exc}") from exc
        if not isinstance(left, str) or not isinstance(right, str):
            raise ValueError(f"{path}: left and right must be SQL strings, got {left!r}/{right!r}")
        return cls(name=raw["name"], key=key, left=left, right=right)

    @classmethod
    def load_all(cls) -> list["ReconciliationConfig"]:
        # No silent-zero here either (see _load_all_table_dir's docstring for
        # the original version of this bug): a vanished config/reconciliations
        # bind mount must fail validate_all, not just quietly stop scheduling
        # reconciliation forever.
        recon_dir = CONFIG_DIR / "reconciliations"
        if not recon_dir.is_dir():
            raise ValueError(f"{recon_dir}: config/reconciliations directory does not exist")
        return [cls.load(path) for path in sorted(recon_dir.glob("*.yml"))]


def fact_event_date_columns() -> dict[str, str]:
    """target ('schema.table') -> event_date_column for every fact_upsert
    layer3 entry across all shipped table configs. Config-driven (ADR-1)
    lookup used by Step 7's historical reload to re-resolve a fact row's
    risk_snapshot_key after a client's dim_client_risk_snapshot history is
    rebuilt — reload.py has no config of its own to read this from since a
    reload iterates clients, not one table (see cdc_historical_reload.py)."""
    result: dict[str, str] = {}
    for cfg in _load_all_table_dir():
        for target in getattr(cfg, "layer3", None) or []:
            if target.strategy == "fact_upsert":
                result[target.target] = target.event_date_column
    return result


def validate_all() -> list[str]:
    """Loads every shipped config, returning names validated. Raises on the
    first invalid one (fail-fast, matching airflow-init's one-shot use), and
    raises if config/tables/*.yml contains zero files — an empty/missing/
    misconfigured directory used to report a fake "0 configs, OK" success
    (Step 2 dual review finding)."""
    table_dir_configs = _load_all_table_dir()
    if not table_dir_configs:
        raise ValueError(f"{CONFIG_DIR / 'tables'}: no config files found (kind: table/"
                          f"derived_dimension/generated_dimension) — check DERIV_CONFIG_DIR"
                          f" and the bind mount before trusting this as a real 'zero tables'")
    validated = [cfg.name for cfg in table_dir_configs]
    validated += [cfg.name for cfg in ReconciliationConfig.load_all()]
    dupes = {name for name in validated if validated.count(name) > 1}
    if dupes:
        # table_name (dq_check_results/dq_table_health) is shared across
        # table configs and reconciliation configs — a name collision would
        # silently merge two unrelated check populations into one regression
        # bucket (Step 9 dual review finding, Sonnet).
        raise ValueError(f"duplicate config name(s) across tables/reconciliations: {sorted(dupes)}")
    return validated


if __name__ == "__main__":
    if "--validate-all" not in sys.argv:
        print(
            "usage: python -m deriv_pipeline.config --validate-all", file=sys.stderr
        )
        sys.exit(2)
    try:
        names = validate_all()
    except Exception as exc:  # noqa: BLE001 - fail-fast CLI entrypoint
        print(f"config validation FAILED: {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
    print(f"config validation OK: {len(names)} config(s) — {', '.join(names)}")
    sys.exit(0)
