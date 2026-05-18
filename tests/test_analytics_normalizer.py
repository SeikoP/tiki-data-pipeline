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

