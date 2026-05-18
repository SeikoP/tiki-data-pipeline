"""Build analysis-ready marts from the core Tiki crawl tables."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from typing import Any

REQUIRED_ANALYTICS_TABLES = {
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

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_schema_name(schema_name: str) -> str:
    """Validate a PostgreSQL schema identifier used in generated SQL."""
    if not _SAFE_IDENTIFIER.fullmatch(schema_name or ""):
        raise ValueError(f"Invalid analytics schema: {schema_name!r}")
    return schema_name


def _q(identifier: str) -> str:
    return f'"{validate_schema_name(identifier)}"'


def _normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.casefold()


def classify_category_path(category_path: Iterable[Any]) -> dict[str, Any]:
    """Classify a category path into the graduation-project home taxonomy."""
    text = " ".join(_normalize_text(part) for part in category_path if part)

    market_scope = "other"
    if "nha cua doi song" in text or "nha bep" in text or "phong an" in text:
        market_scope = "home_living"
    if (
        "dien gia dung" in text
        or "dien tu dien lanh" in text
        or "may giat" in text
        or "tu lanh" in text
    ):
        market_scope = "home_appliance"

    excluded_rules = [
        ("nhac cu", "musical_instrument"),
        ("do tho cung", "religious_item"),
        ("sua chua nha cua", "construction_repair"),
        ("noi that", "large_furniture"),
        ("may giat", "large_appliance"),
        ("tu lanh", "large_appliance"),
        ("may lanh", "large_appliance"),
        ("may dieu hoa", "large_appliance"),
        ("tu dong", "large_appliance"),
        ("tu mat", "large_appliance"),
    ]
    for keyword, reason in excluded_rules:
        if keyword in text:
            return {
                "market_scope": market_scope,
                "home_segment": "excluded_other",
                "is_analysis_target": False,
                "exclusion_reason": reason,
            }

    segment_rules = [
        (("dung cu nha bep", "do dung phong an", "binh dung nuoc"), "kitchen_cooking"),
        (("nha tam", "phong tam"), "bathroom"),
        (("ve sinh", "don dep", "hut bui", "loc khong khi"), "cleaning_homecare"),
        (("gia, moc", "cay treo", "hop dung", "luu tru"), "storage_organization"),
        (("giat", "ui", "say quan ao"), "laundry_garment"),
        (("may nuoc nong", "may rua chen", "quat", "tao am", "may suoi"), "home_comfort"),
        (("trang tri", "den", "chieu sang"), "decor_living"),
    ]
    for keywords, segment in segment_rules:
        if any(keyword in text for keyword in keywords):
            return {
                "market_scope": market_scope,
                "home_segment": segment,
                "is_analysis_target": True,
                "exclusion_reason": None,
            }

    return {
        "market_scope": market_scope,
        "home_segment": "excluded_other",
        "is_analysis_target": False,
        "exclusion_reason": "uncategorized",
    }


def build_schema_sql(schema_name: str) -> list[str]:
    """Return idempotent DDL statements for analytics tables."""
    schema = _q(schema_name)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {schema}",
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."dim_category" (
            category_id VARCHAR(255) PRIMARY KEY,
            category_name VARCHAR(500),
            category_url TEXT,
            level_1 VARCHAR(255),
            level_2 VARCHAR(255),
            level_3 VARCHAR(255),
            level_4 VARCHAR(255),
            level_5 VARCHAR(255),
            root_category_name VARCHAR(255),
            market_scope VARCHAR(50) NOT NULL,
            home_segment VARCHAR(80) NOT NULL,
            is_analysis_target BOOLEAN NOT NULL,
            exclusion_reason VARCHAR(80),
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."dim_seller" (
            seller_id VARCHAR(255) PRIMARY KEY,
            seller_name VARCHAR(500),
            seller_display_name VARCHAR(500) NOT NULL,
            seller_type VARCHAR(50) NOT NULL,
            is_official BOOLEAN,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."dim_product" (
            product_id VARCHAR(255) PRIMARY KEY,
            product_name VARCHAR(1000),
            short_name VARCHAR(255),
            brand VARCHAR(255),
            product_url TEXT,
            image_url TEXT,
            category_id VARCHAR(255),
            seller_id VARCHAR(255),
            market_scope VARCHAR(50) NOT NULL,
            home_segment VARCHAR(80) NOT NULL,
            is_analysis_target BOOLEAN NOT NULL,
            exclusion_reason VARCHAR(80),
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."fact_product_daily" (
            product_id VARCHAR(255) NOT NULL,
            snapshot_date DATE NOT NULL,
            crawl_run_id BIGINT,
            price NUMERIC(12, 2),
            original_price NUMERIC(12, 2),
            discount_percent NUMERIC(7, 2),
            price_bucket VARCHAR(50),
            discount_bucket VARCHAR(50),
            rating_average NUMERIC(4, 2),
            rating_bucket VARCHAR(50),
            review_count INTEGER,
            review_bucket VARCHAR(50),
            sales_count INTEGER,
            sold_bucket VARCHAR(50),
            seller_id VARCHAR(255),
            category_id VARCHAR(255),
            market_scope VARCHAR(50) NOT NULL,
            home_segment VARCHAR(80) NOT NULL,
            is_analysis_target BOOLEAN NOT NULL,
            value_score NUMERIC(8, 2),
            market_position VARCHAR(80),
            recorded_at TIMESTAMP,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (product_id, snapshot_date)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."fact_category_daily" (
            snapshot_date DATE NOT NULL,
            category_id VARCHAR(255) NOT NULL,
            market_scope VARCHAR(50) NOT NULL,
            home_segment VARCHAR(80) NOT NULL,
            product_count INTEGER NOT NULL,
            avg_price NUMERIC(12, 2),
            avg_discount_percent NUMERIC(7, 2),
            avg_rating NUMERIC(4, 2),
            total_reviews BIGINT,
            total_sold BIGINT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (snapshot_date, category_id, home_segment)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."fact_seller_daily" (
            snapshot_date DATE NOT NULL,
            seller_id VARCHAR(255) NOT NULL,
            seller_type VARCHAR(50) NOT NULL,
            market_scope VARCHAR(50) NOT NULL,
            home_segment VARCHAR(80) NOT NULL,
            product_count INTEGER NOT NULL,
            avg_price NUMERIC(12, 2),
            avg_discount_percent NUMERIC(7, 2),
            avg_rating NUMERIC(4, 2),
            total_reviews BIGINT,
            total_sold BIGINT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (snapshot_date, seller_id, home_segment)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."fact_price_change" (
            product_id VARCHAR(255) NOT NULL,
            crawl_run_id BIGINT,
            crawled_at TIMESTAMP NOT NULL,
            price NUMERIC(12, 2),
            previous_price NUMERIC(12, 2),
            price_change NUMERIC(12, 2),
            price_change_percent NUMERIC(7, 2),
            change_direction VARCHAR(20),
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (product_id, crawled_at)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."fact_crawl_quality" (
            crawl_run_id BIGINT PRIMARY KEY,
            airflow_dag_id VARCHAR(255),
            airflow_run_id VARCHAR(255),
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status VARCHAR(30),
            total_raw_items INTEGER,
            total_transformed_items INTEGER,
            total_loaded_items INTEGER,
            raw_product_count INTEGER,
            task_count INTEGER,
            failed_task_count INTEGER,
            detail_product_count INTEGER,
            avg_data_quality_score NUMERIC(6, 2),
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."product_analysis_ready" (
            LIKE {schema}."fact_product_daily" INCLUDING ALL
        )
        """,
    ]


def _execute_many(cursor: Any, statements: Iterable[str]) -> None:
    for statement in statements:
        cursor.execute(statement)


def ensure_analytics_schema(cursor: Any, schema_name: str) -> dict[str, Any]:
    """Ensure analytics schema and tables exist."""
    schema = validate_schema_name(schema_name)
    _execute_many(cursor, build_schema_sql(schema))
    return {"schema": schema, "tables": sorted(REQUIRED_ANALYTICS_TABLES)}


def refresh_analytics_mart(cursor: Any, schema_name: str) -> dict[str, Any]:
    """Refresh the full analytics mart and return row-count stats."""
    ensure_analytics_schema(cursor, schema_name)

    steps = {
        "dimensions": refresh_dimensions(cursor, schema_name),
        "fact_product_daily": refresh_product_daily_fact(cursor, schema_name),
        "fact_category_daily": refresh_category_daily_fact(cursor, schema_name),
        "fact_seller_daily": refresh_seller_daily_fact(cursor, schema_name),
        "fact_price_change": refresh_price_change_fact(cursor, schema_name),
        "fact_crawl_quality": refresh_crawl_quality_fact(cursor, schema_name),
        "product_analysis_ready": refresh_product_analysis_ready(cursor, schema_name),
    }
    return {"schema": validate_schema_name(schema_name), "steps": steps}


def refresh_dimensions(cursor: Any, schema_name: str) -> dict[str, int]:
    """Refresh category, seller, and product dimensions."""
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {
        "dim_category": _refresh_dim_category(cursor, schema),
        "dim_seller": _refresh_dim_seller(cursor, schema),
        "dim_product": _refresh_dim_product(cursor, schema),
    }


def refresh_product_daily_fact(cursor: Any, schema_name: str) -> dict[str, int]:
    """Refresh product daily fact rows."""
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {"fact_product_daily": _refresh_fact_product_daily(cursor, schema)}


def refresh_category_daily_fact(cursor: Any, schema_name: str) -> dict[str, int]:
    """Refresh category daily aggregate fact rows."""
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {"fact_category_daily": _refresh_fact_category_daily(cursor, schema)}


def refresh_seller_daily_fact(cursor: Any, schema_name: str) -> dict[str, int]:
    """Refresh seller daily aggregate fact rows."""
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {"fact_seller_daily": _refresh_fact_seller_daily(cursor, schema)}


def refresh_price_change_fact(cursor: Any, schema_name: str) -> dict[str, int]:
    """Refresh product price-change events."""
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {"fact_price_change": _refresh_fact_price_change(cursor, schema)}


def refresh_crawl_quality_fact(cursor: Any, schema_name: str) -> dict[str, int]:
    """Refresh crawl-run quality metrics."""
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {"fact_crawl_quality": _refresh_fact_crawl_quality(cursor, schema)}


def refresh_product_analysis_ready(cursor: Any, schema_name: str) -> dict[str, int]:
    """Refresh the filtered product table used by the main Power BI dashboard."""
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {"product_analysis_ready": _refresh_product_analysis_ready(cursor, schema)}


def _refresh_dim_category(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        INSERT INTO {schema}."dim_category" (
            category_id, category_name, category_url, level_1, level_2, level_3,
            level_4, level_5, root_category_name, market_scope, home_segment,
            is_analysis_target, exclusion_reason, updated_at
        )
        SELECT
            COALESCE(category_id, tiki_category_id) AS category_id,
            name,
            url,
            level_1,
            level_2,
            level_3,
            level_4,
            level_5,
            root_category_name,
            CASE
                WHEN lower(concat_ws(' ', level_1, root_category_name, name))
                    LIKE '%dien tu dien lanh%' THEN 'home_appliance'
                WHEN lower(concat_ws(' ', level_1, root_category_name, name))
                    LIKE '%dien gia dung%' THEN 'home_appliance'
                WHEN lower(concat_ws(' ', level_1, root_category_name, name))
                    LIKE '%nha cua doi song%' THEN 'home_living'
                WHEN lower(concat_ws(' ', level_1, root_category_name, name))
                    LIKE '%nhà cửa%' THEN 'home_living'
                ELSE 'other'
            END AS market_scope,
            CASE
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%nhạc cụ%'
                    THEN 'excluded_other'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%đồ thờ%'
                    THEN 'excluded_other'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%sửa chữa%'
                    THEN 'excluded_other'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%nội thất%'
                    THEN 'excluded_other'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%máy giặt%'
                    THEN 'excluded_other'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%tủ lạnh%'
                    THEN 'excluded_other'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%máy lạnh%'
                    THEN 'excluded_other'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%dụng cụ nhà bếp%'
                    THEN 'kitchen_cooking'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%phòng ăn%'
                    THEN 'kitchen_cooking'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%nhà tắm%'
                    THEN 'bathroom'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%vệ sinh%'
                    THEN 'cleaning_homecare'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%giá, móc%'
                    THEN 'storage_organization'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%đèn%'
                    THEN 'decor_living'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%trang trí%'
                    THEN 'decor_living'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%máy nước nóng%'
                    THEN 'home_comfort'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%máy rửa chén%'
                    THEN 'home_comfort'
                ELSE 'excluded_other'
            END AS home_segment,
            CASE
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE ANY (
                    ARRAY[
                        '%nhạc cụ%', '%đồ thờ%', '%sửa chữa%', '%nội thất%',
                        '%máy giặt%', '%tủ lạnh%', '%máy lạnh%'
                    ]
                ) THEN FALSE
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE ANY (
                    ARRAY[
                        '%dụng cụ nhà bếp%', '%phòng ăn%', '%nhà tắm%', '%vệ sinh%',
                        '%giá, móc%', '%đèn%', '%trang trí%', '%máy nước nóng%',
                        '%máy rửa chén%'
                    ]
                ) THEN TRUE
                ELSE FALSE
            END AS is_analysis_target,
            CASE
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%nhạc cụ%'
                    THEN 'musical_instrument'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%đồ thờ%'
                    THEN 'religious_item'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%sửa chữa%'
                    THEN 'construction_repair'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%nội thất%'
                    THEN 'large_furniture'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE ANY (
                    ARRAY['%máy giặt%', '%tủ lạnh%', '%máy lạnh%']
                ) THEN 'large_appliance'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE ANY (
                    ARRAY[
                        '%dụng cụ nhà bếp%', '%phòng ăn%', '%nhà tắm%', '%vệ sinh%',
                        '%giá, móc%', '%đèn%', '%trang trí%', '%máy nước nóng%',
                        '%máy rửa chén%'
                    ]
                ) THEN NULL
                ELSE 'uncategorized'
            END AS exclusion_reason,
            CURRENT_TIMESTAMP
        FROM categories
        WHERE COALESCE(category_id, tiki_category_id) IS NOT NULL
        ON CONFLICT (category_id) DO UPDATE SET
            category_name = EXCLUDED.category_name,
            category_url = EXCLUDED.category_url,
            level_1 = EXCLUDED.level_1,
            level_2 = EXCLUDED.level_2,
            level_3 = EXCLUDED.level_3,
            level_4 = EXCLUDED.level_4,
            level_5 = EXCLUDED.level_5,
            root_category_name = EXCLUDED.root_category_name,
            market_scope = EXCLUDED.market_scope,
            home_segment = EXCLUDED.home_segment,
            is_analysis_target = EXCLUDED.is_analysis_target,
            exclusion_reason = EXCLUDED.exclusion_reason,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    return cursor.rowcount


def _refresh_dim_seller(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        INSERT INTO {schema}."dim_seller" (
            seller_id, seller_name, seller_display_name, seller_type, is_official, updated_at
        )
        SELECT
            COALESCE(tiki_seller_id, id::text) AS seller_id,
            name,
            COALESCE(NULLIF(name, ''), 'Unknown Seller') AS seller_display_name,
            CASE
                WHEN is_official IS TRUE THEN 'official'
                WHEN COALESCE(tiki_seller_id, name) IS NULL THEN 'unknown'
                ELSE 'marketplace'
            END AS seller_type,
            is_official,
            CURRENT_TIMESTAMP
        FROM sellers
        WHERE COALESCE(tiki_seller_id, id::text) IS NOT NULL
        ON CONFLICT (seller_id) DO UPDATE SET
            seller_name = EXCLUDED.seller_name,
            seller_display_name = EXCLUDED.seller_display_name,
            seller_type = EXCLUDED.seller_type,
            is_official = EXCLUDED.is_official,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    return cursor.rowcount


def _refresh_dim_product(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        INSERT INTO {schema}."dim_product" (
            product_id, product_name, short_name, brand, product_url, image_url,
            category_id, seller_id, market_scope, home_segment, is_analysis_target,
            exclusion_reason, updated_at
        )
        SELECT
            p.product_id,
            p.name,
            p.short_name,
            p.brand,
            p.url,
            p.image_url,
            p.category_id,
            COALESCE(p.seller_id, p.seller_pk::text),
            COALESCE(dc.market_scope, 'other'),
            COALESCE(dc.home_segment, 'excluded_other'),
            COALESCE(dc.is_analysis_target, FALSE),
            COALESCE(dc.exclusion_reason, 'uncategorized'),
            CURRENT_TIMESTAMP
        FROM products p
        LEFT JOIN {schema}."dim_category" dc ON dc.category_id = p.category_id
        WHERE p.product_id IS NOT NULL
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            short_name = EXCLUDED.short_name,
            brand = EXCLUDED.brand,
            product_url = EXCLUDED.product_url,
            image_url = EXCLUDED.image_url,
            category_id = EXCLUDED.category_id,
            seller_id = EXCLUDED.seller_id,
            market_scope = EXCLUDED.market_scope,
            home_segment = EXCLUDED.home_segment,
            is_analysis_target = EXCLUDED.is_analysis_target,
            exclusion_reason = EXCLUDED.exclusion_reason,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    return cursor.rowcount


def _refresh_fact_product_daily(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        INSERT INTO {schema}."fact_product_daily" (
            product_id, snapshot_date, crawl_run_id, price, original_price,
            discount_percent, price_bucket, discount_bucket, rating_average,
            rating_bucket, review_count, review_bucket, sales_count, sold_bucket,
            seller_id, category_id, market_scope, home_segment, is_analysis_target,
            value_score, market_position, recorded_at, updated_at
        )
        SELECT DISTINCT ON (p.product_id, COALESCE(pp.recorded_at, p.crawled_at)::date)
            p.product_id,
            COALESCE(pp.recorded_at, p.crawled_at)::date AS snapshot_date,
            pp.crawl_run_id,
            CASE WHEN COALESCE(pp.price, p.price) > 0 THEN COALESCE(pp.price, p.price) END,
            CASE
                WHEN COALESCE(pp.original_price, p.original_price) >= COALESCE(pp.price, p.price)
                    THEN COALESCE(pp.original_price, p.original_price)
                ELSE NULL
            END,
            CASE
                WHEN COALESCE(pp.original_price, p.original_price) > 0
                    AND COALESCE(pp.original_price, p.original_price) >= COALESCE(pp.price, p.price)
                    THEN ROUND(
                        (
                            COALESCE(pp.original_price, p.original_price)
                            - COALESCE(pp.price, p.price)
                        ) / COALESCE(pp.original_price, p.original_price) * 100,
                        2
                    )
                ELSE NULL
            END AS discount_percent,
            CASE
                WHEN COALESCE(pp.price, p.price) IS NULL THEN 'price_unknown'
                WHEN COALESCE(pp.price, p.price) < 100000 THEN 'low'
                WHEN COALESCE(pp.price, p.price) < 500000 THEN 'mid'
                WHEN COALESCE(pp.price, p.price) < 2000000 THEN 'high'
                ELSE 'premium'
            END AS price_bucket,
            CASE
                WHEN COALESCE(pp.discount_rate, p.discount_percent) IS NULL THEN 'discount_unknown'
                WHEN COALESCE(pp.discount_rate, p.discount_percent) < 10 THEN 'low_discount'
                WHEN COALESCE(pp.discount_rate, p.discount_percent) < 30 THEN 'mid_discount'
                ELSE 'high_discount'
            END AS discount_bucket,
            COALESCE(pm.rating_average, p.rating_average),
            CASE
                WHEN COALESCE(pm.rating_average, p.rating_average) IS NULL THEN 'rating_unknown'
                WHEN COALESCE(pm.rating_average, p.rating_average) < 3.5 THEN 'rating_low'
                WHEN COALESCE(pm.rating_average, p.rating_average) < 4.5 THEN 'rating_mid'
                ELSE 'rating_high'
            END,
            COALESCE(pm.review_count, p.review_count),
            CASE
                WHEN COALESCE(pm.review_count, p.review_count) IS NULL THEN 'review_unknown'
                WHEN COALESCE(pm.review_count, p.review_count) < 10 THEN 'no_or_low_review'
                WHEN COALESCE(pm.review_count, p.review_count) < 100 THEN 'moderate_review'
                ELSE 'high_review'
            END,
            COALESCE(pm.quantity_sold, p.sales_count),
            CASE
                WHEN COALESCE(pm.quantity_sold, p.sales_count) IS NULL THEN 'sold_unknown'
                WHEN COALESCE(pm.quantity_sold, p.sales_count) < 10 THEN 'low_sold'
                WHEN COALESCE(pm.quantity_sold, p.sales_count) < 100 THEN 'moderate_sold'
                ELSE 'high_sold'
            END,
            dp.seller_id,
            dp.category_id,
            dp.market_scope,
            dp.home_segment,
            dp.is_analysis_target,
            (
                COALESCE(COALESCE(pm.rating_average, p.rating_average), 0) * 20
                + LEAST(COALESCE(pm.review_count, p.review_count, 0), 1000) / 20.0
                + COALESCE(pp.discount_rate, p.discount_percent, 0) / 2.0
            ) AS value_score,
            CASE
                WHEN COALESCE(pp.price, p.price) < 500000
                    AND COALESCE(pm.review_count, p.review_count, 0) >= 100 THEN 'cheap_popular'
                WHEN COALESCE(pp.price, p.price) >= 500000
                    AND COALESCE(pm.rating_average, p.rating_average, 0) >= 4.5
                    THEN 'premium_quality'
                WHEN COALESCE(pp.discount_rate, p.discount_percent, 0) >= 30 THEN 'high_discount'
                WHEN COALESCE(pm.rating_average, p.rating_average, 5) < 3.5
                    OR COALESCE(pm.review_count, p.review_count, 0) < 10 THEN 'low_trust'
                ELSE 'normal'
            END,
            COALESCE(pp.recorded_at, p.crawled_at),
            CURRENT_TIMESTAMP
        FROM products p
        JOIN {schema}."dim_product" dp ON dp.product_id = p.product_id
        LEFT JOIN product_prices pp ON pp.product_id = p.id
        LEFT JOIN product_metrics pm
            ON pm.product_id = p.id
            AND pm.crawl_run_id IS NOT DISTINCT FROM pp.crawl_run_id
        WHERE p.product_id IS NOT NULL
          AND COALESCE(pp.recorded_at, p.crawled_at) IS NOT NULL
        ORDER BY
            p.product_id,
            COALESCE(pp.recorded_at, p.crawled_at)::date,
            COALESCE(pp.recorded_at, p.crawled_at) DESC
        ON CONFLICT (product_id, snapshot_date) DO UPDATE SET
            crawl_run_id = EXCLUDED.crawl_run_id,
            price = EXCLUDED.price,
            original_price = EXCLUDED.original_price,
            discount_percent = EXCLUDED.discount_percent,
            price_bucket = EXCLUDED.price_bucket,
            discount_bucket = EXCLUDED.discount_bucket,
            rating_average = EXCLUDED.rating_average,
            rating_bucket = EXCLUDED.rating_bucket,
            review_count = EXCLUDED.review_count,
            review_bucket = EXCLUDED.review_bucket,
            sales_count = EXCLUDED.sales_count,
            sold_bucket = EXCLUDED.sold_bucket,
            seller_id = EXCLUDED.seller_id,
            category_id = EXCLUDED.category_id,
            market_scope = EXCLUDED.market_scope,
            home_segment = EXCLUDED.home_segment,
            is_analysis_target = EXCLUDED.is_analysis_target,
            value_score = EXCLUDED.value_score,
            market_position = EXCLUDED.market_position,
            recorded_at = EXCLUDED.recorded_at,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    return cursor.rowcount


def _refresh_fact_category_daily(cursor: Any, schema: str) -> int:
    cursor.execute(f'DELETE FROM {schema}."fact_category_daily"')
    cursor.execute(
        f"""
        INSERT INTO {schema}."fact_category_daily" (
            snapshot_date, category_id, market_scope, home_segment, product_count,
            avg_price, avg_discount_percent, avg_rating, total_reviews, total_sold, updated_at
        )
        SELECT
            snapshot_date,
            COALESCE(category_id, 'unknown'),
            market_scope,
            home_segment,
            COUNT(DISTINCT product_id),
            ROUND(AVG(price), 2),
            ROUND(AVG(discount_percent), 2),
            ROUND(AVG(rating_average), 2),
            SUM(review_count),
            SUM(sales_count),
            CURRENT_TIMESTAMP
        FROM {schema}."fact_product_daily"
        GROUP BY snapshot_date, COALESCE(category_id, 'unknown'), market_scope, home_segment
        """
    )
    return cursor.rowcount


def _refresh_fact_seller_daily(cursor: Any, schema: str) -> int:
    cursor.execute(f'DELETE FROM {schema}."fact_seller_daily"')
    cursor.execute(
        f"""
        INSERT INTO {schema}."fact_seller_daily" (
            snapshot_date, seller_id, seller_type, market_scope, home_segment, product_count,
            avg_price, avg_discount_percent, avg_rating, total_reviews, total_sold, updated_at
        )
        SELECT
            f.snapshot_date,
            COALESCE(f.seller_id, 'unknown'),
            COALESCE(ds.seller_type, 'unknown'),
            f.market_scope,
            f.home_segment,
            COUNT(DISTINCT f.product_id),
            ROUND(AVG(f.price), 2),
            ROUND(AVG(f.discount_percent), 2),
            ROUND(AVG(f.rating_average), 2),
            SUM(f.review_count),
            SUM(f.sales_count),
            CURRENT_TIMESTAMP
        FROM {schema}."fact_product_daily" f
        LEFT JOIN {schema}."dim_seller" ds ON ds.seller_id = f.seller_id
        GROUP BY
            f.snapshot_date,
            COALESCE(f.seller_id, 'unknown'),
            COALESCE(ds.seller_type, 'unknown'),
            f.market_scope,
            f.home_segment
        """
    )
    return cursor.rowcount


def _refresh_fact_price_change(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        INSERT INTO {schema}."fact_price_change" (
            product_id, crawl_run_id, crawled_at, price, previous_price,
            price_change, price_change_percent, change_direction, updated_at
        )
        SELECT
            product_id,
            crawl_run_id,
            COALESCE(crawled_at, updated_at, CURRENT_TIMESTAMP),
            price,
            previous_price,
            price_change,
            price_change_percent,
            CASE
                WHEN price_change > 0 THEN 'increase'
                WHEN price_change < 0 THEN 'decrease'
                WHEN price_change = 0 THEN 'unchanged'
                ELSE 'unknown'
            END,
            CURRENT_TIMESTAMP
        FROM crawl_history
        WHERE product_id IS NOT NULL
          AND COALESCE(crawled_at, updated_at) IS NOT NULL
        ON CONFLICT (product_id, crawled_at) DO UPDATE SET
            crawl_run_id = EXCLUDED.crawl_run_id,
            price = EXCLUDED.price,
            previous_price = EXCLUDED.previous_price,
            price_change = EXCLUDED.price_change,
            price_change_percent = EXCLUDED.price_change_percent,
            change_direction = EXCLUDED.change_direction,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    return cursor.rowcount


def _refresh_fact_crawl_quality(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        INSERT INTO {schema}."fact_crawl_quality" (
            crawl_run_id, airflow_dag_id, airflow_run_id, started_at, finished_at,
            status, total_raw_items, total_transformed_items, total_loaded_items,
            raw_product_count, task_count, failed_task_count, detail_product_count,
            avg_data_quality_score, updated_at
        )
        SELECT
            cr.id,
            cr.airflow_dag_id,
            cr.airflow_run_id,
            cr.started_at,
            cr.finished_at,
            cr.status,
            cr.total_raw_items,
            cr.total_transformed_items,
            cr.total_loaded_items,
            COUNT(DISTINCT rp.id),
            COUNT(DISTINCT ct.id),
            COUNT(DISTINCT ct.id) FILTER (WHERE ct.status IN ('failed', 'error')),
            COUNT(DISTINCT p.id) FILTER (WHERE p.detail_status IS NOT NULL),
            ROUND(AVG(p.data_quality_score), 2),
            CURRENT_TIMESTAMP
        FROM crawl_runs cr
        LEFT JOIN raw_products rp ON rp.crawl_run_id = cr.id
        LEFT JOIN crawl_tasks ct ON ct.crawl_run_id = cr.id
        LEFT JOIN product_prices pp ON pp.crawl_run_id = cr.id
        LEFT JOIN products p ON p.id = pp.product_id
        GROUP BY cr.id
        ON CONFLICT (crawl_run_id) DO UPDATE SET
            airflow_dag_id = EXCLUDED.airflow_dag_id,
            airflow_run_id = EXCLUDED.airflow_run_id,
            started_at = EXCLUDED.started_at,
            finished_at = EXCLUDED.finished_at,
            status = EXCLUDED.status,
            total_raw_items = EXCLUDED.total_raw_items,
            total_transformed_items = EXCLUDED.total_transformed_items,
            total_loaded_items = EXCLUDED.total_loaded_items,
            raw_product_count = EXCLUDED.raw_product_count,
            task_count = EXCLUDED.task_count,
            failed_task_count = EXCLUDED.failed_task_count,
            detail_product_count = EXCLUDED.detail_product_count,
            avg_data_quality_score = EXCLUDED.avg_data_quality_score,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    return cursor.rowcount


def _refresh_product_analysis_ready(cursor: Any, schema: str) -> int:
    cursor.execute(f'DELETE FROM {schema}."product_analysis_ready"')
    cursor.execute(
        f"""
        INSERT INTO {schema}."product_analysis_ready"
        SELECT *
        FROM {schema}."fact_product_daily"
        WHERE is_analysis_target = TRUE
        """
    )
    return cursor.rowcount
