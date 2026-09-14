from pathlib import Path

import pytest

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "configs"


def test_parses_minimal_table_config():
    from deriv_pipeline.config import TableConfig

    cfg = TableConfig.load(FIXTURES / "minimal_table.yml")
    assert cfg.kind == "table"
    assert cfg.name == "minimal_table"
    assert cfg.source.natural_key == ["minimal_id"]
    assert cfg.layer1.target == "raw.minimal_table"
    assert cfg.layer2.target == "staging.minimal_table"
    assert len(cfg.layer3) == 1
    assert cfg.layer3[0].strategy == "fact_upsert"


def test_rejects_unknown_layer3_strategy():
    from deriv_pipeline.config import TableConfig

    with pytest.raises(ValueError, match="strategy"):
        TableConfig.load(FIXTURES / "unknown_strategy.yml")


def test_rejects_missing_natural_key():
    from deriv_pipeline.config import TableConfig

    with pytest.raises(ValueError, match="natural_key"):
        TableConfig.load(FIXTURES / "missing_natural_key.yml")


def test_parses_layer2_dq_checks():
    from deriv_pipeline.config import TableConfig

    cfg = TableConfig.load(FIXTURES / "dq_checks.yml")
    assert len(cfg.layer2.dq_checks) == 1
    check = cfg.layer2.dq_checks[0]
    assert check.name == "value_positive"
    assert check.severity == "CRITICAL"
    assert "staging.minimal_table" in check.sql


def test_rejects_dq_check_with_unknown_severity():
    from deriv_pipeline.config import TableConfig

    with pytest.raises(ValueError, match="severity"):
        TableConfig.load(FIXTURES / "dq_checks_bad_severity.yml")


def test_rejects_dq_checks_that_is_not_a_list():
    from deriv_pipeline.config import TableConfig

    with pytest.raises(ValueError, match="dq_checks must be a YAML list"):
        TableConfig.load(FIXTURES / "dq_checks_not_a_list.yml")


def test_rejects_dq_checks_declared_under_layer1():
    from deriv_pipeline.config import TableConfig

    with pytest.raises(ValueError, match="dq_checks is only read from layer2"):
        TableConfig.load(FIXTURES / "dq_checks_under_layer1.yml")


def test_rejects_duplicate_dq_check_names():
    from deriv_pipeline.config import TableConfig

    with pytest.raises(ValueError, match="duplicate dq_checks name"):
        TableConfig.load(FIXTURES / "dq_checks_duplicate_name.yml")


def test_parses_derived_dimension_config():
    from deriv_pipeline.config import DerivedDimensionConfig

    cfg = DerivedDimensionConfig.load(FIXTURES / "derived_dimension.yml")
    assert cfg.kind == "derived_dimension"
    assert cfg.source_table == "client_signup"
    assert cfg.source_column == "assigned_manager"
    assert cfg.target == "warehouse.dim_manager"
    assert cfg.target_key_column == "manager_id"
    assert cfg.raw_source_glob is None
    assert cfg.derived_columns == {}


def test_parses_derived_dimension_config_with_raw_source_glob_and_derived_columns(tmp_path):
    from deriv_pipeline.config import DerivedDimensionConfig

    path = tmp_path / "dim_instrument.yml"
    path.write_text(
        "kind: derived_dimension\nname: dim_instrument\n"
        "raw_source_glob: client_trades.json\nsource_column: instrument\n"
        "target: warehouse.dim_instrument\ntarget_key_column: instrument_name\n"
        "derived_columns:\n  asset_class:\n    EUR/USD: FX\n"
    )
    cfg = DerivedDimensionConfig.load(path)
    assert cfg.source_table is None
    assert cfg.raw_source_glob == "client_trades.json"
    assert cfg.derived_columns == {"asset_class": {"EUR/USD": "FX"}}


def test_derived_dimension_config_requires_exactly_one_source(tmp_path):
    from deriv_pipeline.config import DerivedDimensionConfig

    neither = tmp_path / "neither.yml"
    neither.write_text(
        "kind: derived_dimension\nname: x\nsource_column: y\n"
        "target: warehouse.z\ntarget_key_column: z_id\n"
    )
    with pytest.raises(ValueError, match="source_table or raw_source_glob"):
        DerivedDimensionConfig.load(neither)

    both = tmp_path / "both.yml"
    both.write_text(
        "kind: derived_dimension\nname: x\nsource_table: t\n"
        "raw_source_glob: g.json\nsource_column: y\n"
        "target: warehouse.z\ntarget_key_column: z_id\n"
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        DerivedDimensionConfig.load(both)


def test_parses_generated_dimension_config():
    from deriv_pipeline.config import GeneratedDimensionConfig

    cfg = GeneratedDimensionConfig.load(FIXTURES / "generated_dimension.yml")
    assert cfg.kind == "generated_dimension"
    assert cfg.target == "warehouse.dim_date"
    assert cfg.from_date.isoformat() == "2024-01-01"
    assert cfg.to_date.isoformat() == "2024-12-31"


def test_parses_reconciliation_config():
    from deriv_pipeline.config import ReconciliationConfig

    cfg = ReconciliationConfig.load(FIXTURES / "reconciliation.yml")
    assert cfg.name == "vendor_feed"
    assert cfg.left == "staging.vendor_deposits"
    assert cfg.right == "staging.client_deposit"


def test_all_shipped_configs_validate():
    """Distinct from the fixture-driven tests above: this loads the real
    config/tables/*.yml shipped in the repo, not hand-written fixtures — a
    syntax or strategy typo in a real config must fail this, not just the
    fixture tests."""
    from deriv_pipeline.config import validate_all

    names = validate_all()
    assert "vendor_deposits" in names
    for expected in (
        "client_signup", "client_profile", "dim_manager", "dim_instrument", "dim_date",
        "client_deposit", "client_trades",
    ):
        assert expected in names


def test_rejects_natural_key_as_scalar():
    """natural_key: deposit_id (missing brackets) is a truthy non-empty
    string, so a bare falsiness check silently accepts it and later code
    would iterate its characters instead of one column name — must be
    rejected as loudly as a missing natural_key (Step 2 dual review
    finding, confirmed independently by both reviewers)."""
    from deriv_pipeline.config import TableConfig

    with pytest.raises(ValueError, match="natural_key"):
        TableConfig.load(FIXTURES / "natural_key_scalar.yml")


def test_layer_target_typo_raises_value_error_with_path():
    """A misspelled key in layer1/layer2 (e.g. `targett`) used to raise a
    bare TypeError with no indication of which file was bad — must now
    raise ValueError naming the offending file (Step 2 dual review
    finding)."""
    from deriv_pipeline.config import TableConfig

    path = FIXTURES / "layer1_typo.yml"
    with pytest.raises(ValueError, match=r"layer1_typo\.yml"):
        TableConfig.load(path)


def test_rejects_unknown_kind_in_table_dir(tmp_path, monkeypatch):
    """An unrecognized/typo'd `kind` used to be silently skipped by every
    loader (load_all filtered it out of every kind), so a broken config
    file produced a green validate_all() and a silently-missing DAG — must
    now raise (Step 2 dual review finding, Opus)."""
    import deriv_pipeline.config as config_module

    tables_dir = tmp_path / "tables"
    tables_dir.mkdir()
    (tables_dir / "broken.yml").write_text("kind: tabel\nname: broken\n")
    monkeypatch.setattr(config_module, "CONFIG_DIR", tmp_path)

    with pytest.raises(ValueError, match="unknown kind"):
        config_module.validate_all()


def test_validate_all_raises_on_empty_config_dir(tmp_path, monkeypatch):
    """An empty/missing/misconfigured config/tables directory used to
    report a fake "0 configs, OK" success instead of failing loudly (Step 2
    dual review finding, Opus)."""
    import deriv_pipeline.config as config_module

    (tmp_path / "tables").mkdir()
    monkeypatch.setattr(config_module, "CONFIG_DIR", tmp_path)

    with pytest.raises(ValueError, match="no config files found"):
        config_module.validate_all()


def test_rejects_scd2_baseline_seed_without_cdc_source_glob(tmp_path):
    """A silently-missing cdc_source_glob would make scd2_baseline_seed fall
    back to excluding nobody, fabricating a baseline for an insert-first
    client (e.g. CL030) — must fail at config-load time, not at runtime."""
    from deriv_pipeline.config import TableConfig

    path = tmp_path / "bad_baseline_seed.yml"
    path.write_text(
        "kind: table\nname: bad\n"
        "source: {format: json, glob: 'x.json', natural_key: [client_id]}\n"
        "layer1: {target: raw.x}\nlayer2: {target: staging.x}\n"
        "layer3:\n  - {target: warehouse.dim_client_risk_snapshot, strategy: scd2_baseline_seed}\n"
    )
    with pytest.raises(ValueError, match="cdc_source_glob"):
        TableConfig.load(path)


def test_rejects_fact_upsert_without_event_date_column(tmp_path):
    """Without event_date_column, fact_upsert puts a bare None into its
    generated column list and raises a confusing TypeError deep in layer3 at
    DAG runtime instead of failing loudly at config-load time (Step 5 dual
    review finding)."""
    from deriv_pipeline.config import TableConfig

    path = tmp_path / "bad_fact_upsert.yml"
    path.write_text(
        "kind: table\nname: bad\n"
        "source: {format: json, glob: 'x.json', natural_key: [x_id]}\n"
        "layer1: {target: raw.x}\nlayer2: {target: staging.x}\n"
        "layer3:\n  - {target: warehouse.fact_x, strategy: fact_upsert}\n"
    )
    with pytest.raises(ValueError, match="event_date_column"):
        TableConfig.load(path)


def test_rejects_fact_upsert_literals_as_scalar(tmp_path):
    """literals: source_system (missing braces) is the same scalar-vs-list
    footgun _require_str_list already guards against elsewhere — must be
    rejected as loudly, not silently treated as truthy and misused as a dict
    later (`literals.keys()`/`**literals` would raise a confusing
    AttributeError deep in fact_upsert instead)."""
    from deriv_pipeline.config import TableConfig

    path = tmp_path / "bad_literals.yml"
    path.write_text(
        "kind: table\nname: bad\n"
        "source: {format: json, glob: 'x.json', natural_key: [x_id]}\n"
        "layer1: {target: raw.x}\nlayer2: {target: staging.x}\n"
        "layer3:\n  - {target: warehouse.fact_x, strategy: fact_upsert,"
        " event_date_column: x_date, literals: source_system}\n"
    )
    with pytest.raises(ValueError, match="literals"):
        TableConfig.load(path)


def test_rejects_fact_upsert_dict_fk_resolution_missing_keys(tmp_path):
    """A dict-shaped fk_resolution rule missing one of its four required
    keys must fail at config-load time with a clear message, not a bare
    KeyError deep inside fact_upsert at DAG runtime."""
    from deriv_pipeline.config import TableConfig

    path = tmp_path / "bad_fk_rule.yml"
    path.write_text(
        "kind: table\nname: bad\n"
        "source: {format: json, glob: 'x.json', natural_key: [x_id]}\n"
        "layer1: {target: raw.x}\nlayer2: {target: staging.x}\n"
        "layer3:\n  - target: warehouse.fact_x\n    strategy: fact_upsert\n"
        "    event_date_column: x_date\n"
        "    fk_resolution:\n      instrument_key: {from_column: instrument}\n"
    )
    with pytest.raises(ValueError, match="fk_resolution"):
        TableConfig.load(path)


def test_rejects_fact_upsert_fk_resolution_typo_sentinel(tmp_path):
    """A typo'd sentinel (e.g. "inferred_member" instead of
    "inferred_member_on_miss") must not be silently accepted — it would
    silently flip fact_upsert's behavior from "create an inferred stub" to
    "raise on any unknown client" with no error at config-load time (Step 5
    dual review finding, Opus)."""
    from deriv_pipeline.config import TableConfig

    path = tmp_path / "bad_sentinel.yml"
    path.write_text(
        "kind: table\nname: bad\n"
        "source: {format: json, glob: 'x.json', natural_key: [x_id]}\n"
        "layer1: {target: raw.x}\nlayer2: {target: staging.x}\n"
        "layer3:\n  - {target: warehouse.fact_x, strategy: fact_upsert,"
        " event_date_column: x_date, fk_resolution: {dim_client: inferred_member}}\n"
    )
    with pytest.raises(ValueError, match="fk_resolution"):
        TableConfig.load(path)


def test_rejects_generated_dimension_from_after_to():
    from deriv_pipeline.config import GeneratedDimensionConfig

    bad = FIXTURES / "generated_dimension_inverted.yml"
    bad.write_text(
        "kind: generated_dimension\nname: dim_date_bad\n"
        "target: warehouse.dim_date\nfrom: '2024-12-31'\nto: '2024-01-01'\n"
    )
    try:
        with pytest.raises(ValueError, match="after"):
            GeneratedDimensionConfig.load(bad)
    finally:
        bad.unlink()
