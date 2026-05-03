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
    indexes = "\n".join(db_schema.INDEX_STATEMENTS)

    assert "UNIQUE (crawl_run_id, tiki_product_id, source_stage)" in ddl
    assert "UNIQUE (product_id, crawl_run_id)" in ddl
    assert "raw_json JSONB NOT NULL" in ddl
    assert "crawl_run_id BIGINT REFERENCES crawl_runs(id)" in ddl
    assert "ux_history_product_run" in indexes


def test_layered_schema_has_product_detail_quality_columns() -> None:
    ddl = "\n".join(db_schema.TABLE_STATEMENTS + db_schema.COLUMN_STATEMENTS)

    for column in (
        "images JSONB",
        "description_text TEXT",
        "detail_status VARCHAR(50)",
        "missing_fields JSONB",
        "data_quality_score DECIMAL(4, 2)",
    ):
        assert column in ddl

    assert "specifications JSONB" not in ddl
    assert "parent_id BIGINT" not in ddl
    assert "url TEXT,\n        rating DECIMAL" not in ddl
    assert "page INTEGER" not in ddl
    assert "level_4 VARCHAR" in ddl
    assert "level_5 VARCHAR" in ddl
