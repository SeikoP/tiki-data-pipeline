from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def test_validate_schema_name_accepts_safe_identifier():
    from pipelines.analytics.normalizer import validate_schema_name

    assert validate_schema_name("analytics") == "analytics"
    assert validate_schema_name("analytics_mart_2026") == "analytics_mart_2026"


@pytest.mark.parametrize("schema", ["1analytics", "analytics.mart", "analytics;drop", ""])
def test_validate_schema_name_rejects_unsafe_identifier(schema):
    from pipelines.analytics.normalizer import validate_schema_name

    with pytest.raises(ValueError, match="Invalid analytics schema"):
        validate_schema_name(schema)


def test_analytics_schema_sql_contains_required_tables():
    from pipelines.analytics.normalizer import REQUIRED_ANALYTICS_TABLES, build_schema_sql

    ddl = "\n".join(build_schema_sql("analytics"))

    assert REQUIRED_ANALYTICS_TABLES == {
        "dim_category",
        "dim_seller",
        "dim_product",
        "fact_product_daily",
        "fact_category_daily",
        "fact_seller_daily",
        "fact_price_change",
        "fact_crawl_quality",
        "product_analysis_ready",
    }
    for table_name in REQUIRED_ANALYTICS_TABLES:
        assert f'CREATE TABLE IF NOT EXISTS "analytics"."{table_name}"' in ddl


def test_analytics_schema_sql_creates_bi_mart_views():
    from pipelines.analytics.normalizer import build_schema_sql

    ddl = "\n".join(build_schema_sql("analytics"))

    assert 'CREATE OR REPLACE VIEW "analytics"."mart_product_wide"' in ddl
    assert 'CREATE OR REPLACE VIEW "analytics"."mart_category_summary"' in ddl
    assert 'CREATE OR REPLACE VIEW "analytics"."mart_seller_summary"' in ddl
    assert 'CREATE OR REPLACE VIEW "analytics"."mart_crawl_quality"' in ddl
    assert "COALESCE(s.seller_display_name, 'Unknown Seller')" in ddl
    assert "home_segment_label" in ddl
    assert "market_scope_label" in ddl


def test_analytics_schema_sql_contains_analysis_labels_and_statuses():
    from pipelines.analytics.normalizer import build_schema_sql

    ddl = "\n".join(build_schema_sql("analytics"))

    expected_columns = [
        "price_bucket_label",
        "price_status",
        "price_status_label",
        "discount_bucket_label",
        "discount_status",
        "discount_status_label",
        "rating_bucket_label",
        "rating_status",
        "rating_status_label",
        "review_bucket_label",
        "review_status",
        "review_status_label",
        "sold_bucket_label",
        "sold_status",
        "sold_status_label",
        "market_scope_label",
        "home_segment_label",
        "market_position_label",
        "exclusion_reason_label",
        "seller_type_label",
        "change_direction_label",
        "status_label",
    ]
    for column_name in expected_columns:
        assert column_name in ddl

    assert 'ALTER TABLE "analytics"."fact_product_daily"' in ddl
    assert 'ALTER TABLE "analytics"."product_analysis_ready"' in ddl


def test_analytics_fact_sql_normalizes_missing_seller_to_unknown():
    from pipelines.analytics.normalizer import _refresh_fact_product_daily

    class DummyCursor:
        def __init__(self):
            self.statements = []
            self.rowcount = 0

        def execute(self, sql):
            self.statements.append(sql)

    cursor = DummyCursor()
    _refresh_fact_product_daily(cursor, '"analytics"')
    sql = "\n".join(cursor.statements)

    assert "COALESCE(dp.seller_id, 'unknown')" in sql


def test_product_daily_refresh_distinguishes_missing_zero_and_labels():
    from pipelines.analytics.normalizer import _refresh_fact_product_daily

    class Cursor:
        rowcount = 0

        def __init__(self):
            self.statements = []

        def execute(self, sql):
            self.statements.append(sql)

    cursor = Cursor()
    _refresh_fact_product_daily(cursor, '"analytics"')
    sql = "\n".join(cursor.statements)

    assert len(cursor.statements) == 2
    assert "WHEN raw_price IS NULL THEN 'price_unknown'" in sql
    assert "WHEN raw_price = 0 THEN 'price_zero'" in sql
    assert "WHEN rating_average IS NULL THEN 'missing_rating'" in sql
    assert "WHEN rating_average = 0 THEN 'zero_rating'" in sql
    assert "WHEN review_count IS NULL THEN 'missing_review_count'" in sql
    assert "WHEN review_count = 0 THEN 'zero_review'" in sql
    assert "WHEN sales_count IS NULL THEN 'missing_sales_count'" in sql
    assert "WHEN sales_count = 0 THEN 'zero_sold'" in sql
    assert "Thiếu tín hiệu" in sql
    assert "Nhà cửa" in sql
    assert "Bếp" in sql


@pytest.mark.parametrize(
    ("path", "expected_segment", "expected_target", "expected_reason"),
    [
        (["Nha Cua Doi Song", "Dụng cụ nhà bếp", "Nồi"], "kitchen_cooking", True, None),
        (
            ["Nha Cua Doi Song", "Đồ dùng và thiết bị nhà tắm", "Kệ phòng tắm"],
            "bathroom",
            True,
            None,
        ),
        (
            ["Nha Cua Doi Song", "Đồ dùng và thiết bị nhà tắm", "Dụng cụ vệ sinh"],
            "cleaning_homecare",
            True,
            None,
        ),
        (
            ["Nha Cua Doi Song", "Đồ dùng và thiết bị nhà tắm", "Phụ kiện giặt ủi"],
            "laundry_garment",
            True,
            None,
        ),
        (
            ["Dien Tu Dien Lanh", "Máy giặt", "Máy giặt cửa trước"],
            "excluded_other",
            False,
            "large_appliance",
        ),
        (
            ["Nha Cua Doi Song", "Nhạc cụ", "Amply"],
            "excluded_other",
            False,
            "musical_instrument",
        ),
    ],
)
def test_classify_category_path_for_graduation_scope(
    path, expected_segment, expected_target, expected_reason
):
    from pipelines.analytics.normalizer import classify_category_path

    result = classify_category_path(path)

    assert result["home_segment"] == expected_segment
    assert result["is_analysis_target"] is expected_target
    assert result["exclusion_reason"] == expected_reason
