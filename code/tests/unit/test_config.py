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
