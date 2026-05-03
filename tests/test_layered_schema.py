from __future__ import annotations

from src.common import db_schema


def test_layered_schema_declares_required_tables() -> None:
    expected = {
        "crawl_runs",
        "crawl_tasks",
        "raw_products",
        "sellers",
        "categories",
        "products",
        "product_prices",
        "product_metrics",
    }

    assert expected <= db_schema.REQUIRED_TABLES


def test_layered_schema_has_raw_and_snapshot_uniqueness() -> None:
    ddl = "\n".join(db_schema.TABLE_STATEMENTS)

    assert "UNIQUE (crawl_run_id, tiki_product_id, source_stage)" in ddl
    assert "UNIQUE (product_id, crawl_run_id)" in ddl
    assert "raw_json JSONB NOT NULL" in ddl
