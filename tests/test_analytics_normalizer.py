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

    assert REQUIRED_ANALYTICS_TABLES == {"product_analysis_ready"}
    for table_name in REQUIRED_ANALYTICS_TABLES:
        assert f'CREATE TABLE "analytics"."{table_name}"' in ddl


def test_analytics_schema_sql_removes_legacy_operational_tables():
    from pipelines.analytics.normalizer import build_schema_sql

    ddl = "\n".join(build_schema_sql("analytics"))

    legacy_objects = [
        "fact_product_daily",
        "fact_category_daily",
        "fact_seller_daily",
        "fact_price_change",
        "fact_crawl_quality",
        "dim_product",
        "dim_category",
        "dim_seller",
        "mart_product_wide",
        "mart_crawl_quality",
        "category_summary",
        "seller_summary",
    ]
    for object_name in legacy_objects:
        assert f'DROP TABLE IF EXISTS "analytics"."{object_name}"' in ddl or (
            f'DROP VIEW IF EXISTS "analytics"."{object_name}"' in ddl
        )

    assert 'CREATE TABLE "analytics"."fact_product_daily"' not in ddl
    assert 'CREATE TABLE "analytics"."fact_crawl_quality"' not in ddl
    assert 'CREATE VIEW "analytics"."mart_crawl_quality"' not in ddl


def test_analytics_schema_sql_has_no_extra_label_columns():
    from pipelines.analytics.normalizer import build_schema_sql

    ddl = "\n".join(build_schema_sql("analytics"))

    removed_columns = [
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
    for column_name in removed_columns:
        assert column_name not in ddl


def test_product_analysis_ready_refresh_builds_clean_current_snapshot():
    from pipelines.analytics.normalizer import _refresh_product_analysis_ready

    class Cursor:
        rowcount = 0

        def __init__(self):
            self.statements = []

        def execute(self, sql):
            self.statements.append(sql)

    cursor = Cursor()
    _refresh_product_analysis_ready(cursor, '"analytics"')
    sql = "\n".join(cursor.statements)

    assert len(cursor.statements) == 2
    assert 'INSERT INTO "analytics"."product_analysis_ready"' in sql
    assert "seller_id IS NOT NULL" in sql
    assert "seller_name IS NOT NULL" in sql
    assert "home_segment <> 'Ngoài scope'" in sql
    assert "price > 0" in sql
    assert "COALESCE(f.seller_id, 'unknown')" not in sql
    assert "Unknown Seller" not in sql
    assert "Giá TB" in sql
    assert "KM cao" in sql
    assert "Rating 0" in sql
    assert "0 review" in sql
    assert "0 sold" in sql
    assert "Thiếu tín hiệu" in sql
    assert "Nhà cửa" in sql
    assert "Bếp" in sql


@pytest.mark.parametrize(
    ("path", "expected_segment", "expected_target", "expected_reason"),
    [
        (["Nha Cua Doi Song", "Dụng cụ nhà bếp", "Nồi"], "Bếp", True, None),
        (
            ["Nha Cua Doi Song", "Đồ dùng và thiết bị nhà tắm", "Kệ phòng tắm"],
            "Nhà tắm",
            True,
            None,
        ),
        (
            ["Nha Cua Doi Song", "Đồ dùng và thiết bị nhà tắm", "Dụng cụ vệ sinh"],
            "Vệ sinh",
            True,
            None,
        ),
        (
            ["Nha Cua Doi Song", "Đồ dùng và thiết bị nhà tắm", "Phụ kiện giặt ủi"],
            "Giặt ủi",
            True,
            None,
        ),
        (
            ["Dien Tu Dien Lanh", "Máy giặt", "Máy giặt cửa trước"],
            "Ngoài scope",
            False,
            "Điện máy lớn",
        ),
        (
            ["Nha Cua Doi Song", "Nhạc cụ", "Amply"],
            "Ngoài scope",
            False,
            "Nhạc cụ",
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
