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


def test_parses_derived_dimension_config():
    from deriv_pipeline.config import DerivedDimensionConfig

    cfg = DerivedDimensionConfig.load(FIXTURES / "derived_dimension.yml")
    assert cfg.kind == "derived_dimension"
    assert cfg.source_table == "client_signup"
    assert cfg.source_column == "assigned_manager"
    assert cfg.target == "warehouse.dim_manager"


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
