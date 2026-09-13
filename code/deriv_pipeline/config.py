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
class LayerTarget:
    target: str
    conflict_strategy: str | None = None
    update_columns: list[str] = field(default_factory=list)
    ge_suite: str | None = None


@dataclass
class Layer3Target:
    target: str
    strategy: str
    fk_resolution: dict = field(default_factory=dict)
    columns: list[str] | None = None


def _build_layer_target(raw: dict, path: Path, label: str) -> LayerTarget:
    try:
        update_columns = _require_str_list(raw, "update_columns", path, required=False)
        return LayerTarget(
            target=raw["target"],
            conflict_strategy=raw.get("conflict_strategy"),
            update_columns=update_columns,
            ge_suite=raw.get("ge_suite"),
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
            try:
                columns = _require_str_list(entry, "columns", path, required=False) or None
                layer3.append(
                    Layer3Target(
                        target=entry["target"],
                        strategy=strategy,
                        fk_resolution=entry.get("fk_resolution", {}),
                        columns=columns,
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
    source_table: str
    source_column: str
    target: str

    @classmethod
    def load(cls, path: Path) -> "DerivedDimensionConfig":
        path = Path(path)
        raw = _load_yaml(path)
        try:
            return cls(
                kind=raw["kind"],
                name=raw["name"],
                source_table=raw["source_table"],
                source_column=raw["source_column"],
                target=raw["target"],
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
    name: str
    left: str
    right: str
    key: str
    compare_columns: list[str]

    @classmethod
    def load(cls, path: Path) -> "ReconciliationConfig":
        path = Path(path)
        raw = _load_yaml(path)
        try:
            return cls(
                name=raw["name"],
                left=raw["left"],
                right=raw["right"],
                key=raw["key"],
                compare_columns=_require_str_list(
                    raw, "compare_columns", path, required=False
                ),
            )
        except KeyError as exc:
            raise ValueError(f"{path}: missing required key {exc}") from exc

    @classmethod
    def load_all(cls) -> list["ReconciliationConfig"]:
        recon_dir = CONFIG_DIR / "reconciliations"
        if not recon_dir.is_dir():
            return []
        return [cls.load(path) for path in sorted(recon_dir.glob("*.yml"))]


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
