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
    text = text.replace("đ", "d").replace("Đ", "D")
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
        (("ve sinh", "don dep", "hut bui", "loc khong khi"), "cleaning_homecare"),
        (("giat", "ui", "say quan ao"), "laundry_garment"),
        (("gia, moc", "cay treo", "hop dung", "luu tru"), "storage_organization"),
        (("nha tam", "phong tam"), "bathroom"),
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
            market_scope_label VARCHAR(100),
            home_segment VARCHAR(80) NOT NULL,
            home_segment_label VARCHAR(100),
            is_analysis_target BOOLEAN NOT NULL,
            exclusion_reason VARCHAR(80),
            exclusion_reason_label VARCHAR(120),
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."dim_seller" (
            seller_id VARCHAR(255) PRIMARY KEY,
            seller_name VARCHAR(500),
            seller_display_name VARCHAR(500) NOT NULL,
            seller_type VARCHAR(50) NOT NULL,
            seller_type_label VARCHAR(100),
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
            market_scope_label VARCHAR(100),
            home_segment VARCHAR(80) NOT NULL,
            home_segment_label VARCHAR(100),
            is_analysis_target BOOLEAN NOT NULL,
            exclusion_reason VARCHAR(80),
            exclusion_reason_label VARCHAR(120),
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
            price_bucket_label VARCHAR(100),
            price_status VARCHAR(50),
            price_status_label VARCHAR(100),
            discount_bucket VARCHAR(50),
            discount_bucket_label VARCHAR(100),
            discount_status VARCHAR(50),
            discount_status_label VARCHAR(100),
            rating_average NUMERIC(4, 2),
            rating_bucket VARCHAR(50),
            rating_bucket_label VARCHAR(100),
            rating_status VARCHAR(50),
            rating_status_label VARCHAR(100),
            review_count INTEGER,
            review_bucket VARCHAR(50),
            review_bucket_label VARCHAR(100),
            review_status VARCHAR(50),
            review_status_label VARCHAR(100),
            sales_count INTEGER,
            sold_bucket VARCHAR(50),
            sold_bucket_label VARCHAR(100),
            sold_status VARCHAR(50),
            sold_status_label VARCHAR(100),
            seller_id VARCHAR(255),
            category_id VARCHAR(255),
            market_scope VARCHAR(50) NOT NULL,
            market_scope_label VARCHAR(100),
            home_segment VARCHAR(80) NOT NULL,
            home_segment_label VARCHAR(100),
            is_analysis_target BOOLEAN NOT NULL,
            value_score NUMERIC(8, 2),
            market_position VARCHAR(80),
            market_position_label VARCHAR(120),
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
            market_scope_label VARCHAR(100),
            home_segment VARCHAR(80) NOT NULL,
            home_segment_label VARCHAR(100),
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
            seller_type_label VARCHAR(100),
            market_scope VARCHAR(50) NOT NULL,
            market_scope_label VARCHAR(100),
            home_segment VARCHAR(80) NOT NULL,
            home_segment_label VARCHAR(100),
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
            change_direction_label VARCHAR(100),
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
            status_label VARCHAR(100),
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
        *[
            f"""
            ALTER TABLE {schema}."{table_name}"
                ADD COLUMN IF NOT EXISTS price_bucket_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS price_status VARCHAR(50),
                ADD COLUMN IF NOT EXISTS price_status_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS discount_bucket_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS discount_status VARCHAR(50),
                ADD COLUMN IF NOT EXISTS discount_status_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS rating_bucket_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS rating_status VARCHAR(50),
                ADD COLUMN IF NOT EXISTS rating_status_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS review_bucket_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS review_status VARCHAR(50),
                ADD COLUMN IF NOT EXISTS review_status_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS sold_bucket_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS sold_status VARCHAR(50),
                ADD COLUMN IF NOT EXISTS sold_status_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS market_scope_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS home_segment_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS market_position_label VARCHAR(120)
            """
            for table_name in ("fact_product_daily", "product_analysis_ready")
        ],
        *[
            f"""
            ALTER TABLE {schema}."{table_name}"
                ADD COLUMN IF NOT EXISTS market_scope_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS home_segment_label VARCHAR(100),
                ADD COLUMN IF NOT EXISTS exclusion_reason_label VARCHAR(120)
            """
            for table_name in ("dim_category", "dim_product")
        ],
        f"""
        ALTER TABLE {schema}."dim_seller"
            ADD COLUMN IF NOT EXISTS seller_type_label VARCHAR(100)
        """,
        f"""
        ALTER TABLE {schema}."fact_category_daily"
            ADD COLUMN IF NOT EXISTS market_scope_label VARCHAR(100),
            ADD COLUMN IF NOT EXISTS home_segment_label VARCHAR(100)
        """,
        f"""
        ALTER TABLE {schema}."fact_seller_daily"
            ADD COLUMN IF NOT EXISTS seller_type_label VARCHAR(100),
            ADD COLUMN IF NOT EXISTS market_scope_label VARCHAR(100),
            ADD COLUMN IF NOT EXISTS home_segment_label VARCHAR(100)
        """,
        f"""
        ALTER TABLE {schema}."fact_price_change"
            ADD COLUMN IF NOT EXISTS change_direction_label VARCHAR(100)
        """,
        f"""
        ALTER TABLE {schema}."fact_crawl_quality"
            ADD COLUMN IF NOT EXISTS status_label VARCHAR(100)
        """,
        *_build_mart_view_sql(schema),
    ]


def _build_mart_view_sql(schema: str) -> list[str]:
    """Return idempotent BI-facing mart view DDL."""
    return [
        f'DROP VIEW IF EXISTS {schema}."mart_product_wide"',
        f'DROP VIEW IF EXISTS {schema}."mart_product_analysis_ready"',
        f'DROP VIEW IF EXISTS {schema}."mart_category_summary"',
        f'DROP VIEW IF EXISTS {schema}."mart_seller_summary"',
        f'DROP VIEW IF EXISTS {schema}."mart_crawl_quality"',
        f"""
        CREATE OR REPLACE VIEW {schema}."mart_product_analysis_ready" AS
        SELECT *
        FROM {schema}."product_analysis_ready"
        """,
        f"""
        CREATE OR REPLACE VIEW {schema}."mart_product_wide" AS
        SELECT
            f.product_id,
            p.product_name,
            p.short_name,
            p.brand,
            f.category_id,
            COALESCE(c.category_name, 'Unknown Category') AS category_name,
            c.level_1,
            c.level_2,
            c.level_3,
            c.level_4,
            COALESCE(c.exclusion_reason, f.market_position) AS exclusion_reason,
            COALESCE(c.exclusion_reason_label, 'Trong scope') AS exclusion_reason_label,
            COALESCE(f.seller_id, 'unknown') AS seller_id,
            COALESCE(s.seller_display_name, 'Unknown Seller') AS seller_display_name,
            COALESCE(s.seller_type, 'unknown') AS seller_type,
            COALESCE(s.seller_type_label, 'Không rõ') AS seller_type_label,
            f.snapshot_date,
            f.home_segment,
            f.home_segment_label,
            f.market_scope,
            f.market_scope_label,
            f.price,
            f.original_price,
            f.discount_percent,
            f.price_bucket,
            f.price_bucket_label,
            f.price_status,
            f.price_status_label,
            f.discount_bucket,
            f.discount_bucket_label,
            f.discount_status,
            f.discount_status_label,
            f.rating_average,
            f.rating_bucket,
            f.rating_bucket_label,
            f.rating_status,
            f.rating_status_label,
            f.review_count,
            f.review_bucket,
            f.review_bucket_label,
            f.review_status,
            f.review_status_label,
            f.sales_count,
            f.sold_bucket,
            f.sold_bucket_label,
            f.sold_status,
            f.sold_status_label,
            f.value_score,
            f.market_position,
            f.market_position_label,
            p.product_url,
            p.image_url
        FROM {schema}."product_analysis_ready" f
        LEFT JOIN {schema}."dim_product" p ON p.product_id = f.product_id
        LEFT JOIN {schema}."dim_category" c ON c.category_id = f.category_id
        LEFT JOIN {schema}."dim_seller" s ON s.seller_id = COALESCE(f.seller_id, 'unknown')
        """,
        f"""
        CREATE OR REPLACE VIEW {schema}."mart_category_summary" AS
        SELECT
            snapshot_date,
            home_segment,
            home_segment_label,
            market_scope,
            market_scope_label,
            COUNT(DISTINCT category_id) AS category_count,
            SUM(product_count) AS product_count,
            ROUND(AVG(avg_price), 0) AS avg_price,
            ROUND(AVG(avg_discount_percent), 2) AS avg_discount_percent,
            ROUND(AVG(avg_rating), 2) AS avg_rating,
            SUM(total_reviews) AS total_reviews,
            SUM(total_sold) AS total_sold
        FROM {schema}."fact_category_daily"
        GROUP BY
            snapshot_date,
            home_segment,
            home_segment_label,
            market_scope,
            market_scope_label
        """,
        f"""
        CREATE OR REPLACE VIEW {schema}."mart_seller_summary" AS
        SELECT
            f.snapshot_date,
            COALESCE(f.seller_id, 'unknown') AS seller_id,
            COALESCE(s.seller_display_name, 'Unknown Seller') AS seller_display_name,
            COALESCE(f.seller_type, 'unknown') AS seller_type,
            COALESCE(f.seller_type_label, 'Không rõ') AS seller_type_label,
            f.home_segment,
            f.home_segment_label,
            f.market_scope,
            f.market_scope_label,
            f.product_count,
            f.avg_price,
            f.avg_discount_percent,
            f.avg_rating,
            f.total_reviews,
            f.total_sold
        FROM {schema}."fact_seller_daily" f
        LEFT JOIN {schema}."dim_seller" s ON s.seller_id = COALESCE(f.seller_id, 'unknown')
        """,
        f"""
        CREATE OR REPLACE VIEW {schema}."mart_crawl_quality" AS
        SELECT
            crawl_run_id,
            airflow_dag_id,
            airflow_run_id,
            started_at,
            finished_at,
            status,
            status_label,
            total_raw_items,
            total_transformed_items,
            total_loaded_items,
            raw_product_count,
            task_count,
            failed_task_count,
            detail_product_count,
            avg_data_quality_score,
            updated_at
        FROM {schema}."fact_crawl_quality"
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
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%vệ sinh%'
                    THEN 'cleaning_homecare'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%dọn dẹp%'
                    THEN 'cleaning_homecare'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%hút bụi%'
                    THEN 'cleaning_homecare'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%giặt%'
                    THEN 'laundry_garment'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%ủi%'
                    THEN 'laundry_garment'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%sấy quần áo%'
                    THEN 'laundry_garment'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%giá, móc%'
                    THEN 'storage_organization'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%cây treo%'
                    THEN 'storage_organization'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%hộp đựng%'
                    THEN 'storage_organization'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%lưu trữ%'
                    THEN 'storage_organization'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%nhà tắm%'
                    THEN 'bathroom'
                WHEN lower(concat_ws(' ', level_2, level_3, name)) LIKE '%phòng tắm%'
                    THEN 'bathroom'
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
                        '%dụng cụ nhà bếp%', '%phòng ăn%', '%nhà tắm%', '%phòng tắm%',
                        '%vệ sinh%', '%dọn dẹp%', '%hút bụi%', '%giặt%', '%ủi%',
                        '%sấy quần áo%', '%giá, móc%', '%cây treo%', '%hộp đựng%',
                        '%lưu trữ%', '%đèn%', '%trang trí%', '%máy nước nóng%',
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
    refreshed_rows = cursor.rowcount
    _backfill_dim_category_labels(cursor, schema)
    return refreshed_rows


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
    refreshed_rows = cursor.rowcount
    cursor.execute(
        f"""
        INSERT INTO {schema}."dim_seller" (
            seller_id, seller_name, seller_display_name, seller_type, is_official, updated_at
        )
        VALUES ('unknown', NULL, 'Unknown Seller', 'unknown', NULL, CURRENT_TIMESTAMP)
        ON CONFLICT (seller_id) DO UPDATE SET
            seller_name = EXCLUDED.seller_name,
            seller_display_name = EXCLUDED.seller_display_name,
            seller_type = EXCLUDED.seller_type,
            is_official = EXCLUDED.is_official,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    _backfill_dim_seller_labels(cursor, schema)
    return refreshed_rows


def _label_market_scope_sql(column: str = "market_scope") -> str:
    return f"""
        CASE
            WHEN {column} = 'home_living' THEN 'Nhà cửa'
            WHEN {column} = 'home_appliance' THEN 'Điện GD'
            WHEN {column} = 'mixed' THEN 'Đa scope'
            ELSE 'Khác'
        END
    """


def _label_home_segment_sql(column: str = "home_segment") -> str:
    return f"""
        CASE
            WHEN {column} = 'kitchen_cooking' THEN 'Bếp'
            WHEN {column} = 'cleaning_homecare' THEN 'Vệ sinh'
            WHEN {column} = 'laundry_garment' THEN 'Giặt ủi'
            WHEN {column} = 'storage_organization' THEN 'Lưu trữ'
            WHEN {column} = 'bathroom' THEN 'Nhà tắm'
            WHEN {column} = 'home_comfort' THEN 'Tiện nghi'
            WHEN {column} = 'decor_living' THEN 'Trang trí'
            ELSE 'Ngoài scope'
        END
    """


def _label_exclusion_reason_sql(column: str = "exclusion_reason") -> str:
    return f"""
        CASE
            WHEN {column} IS NULL THEN 'Trong scope'
            WHEN {column} = 'large_appliance' THEN 'Điện máy lớn'
            WHEN {column} = 'large_furniture' THEN 'Nội thất lớn'
            WHEN {column} = 'musical_instrument' THEN 'Nhạc cụ'
            WHEN {column} = 'religious_item' THEN 'Đồ thờ'
            WHEN {column} = 'construction_repair' THEN 'Sửa chữa'
            WHEN {column} = 'uncategorized' THEN 'Chưa phân loại'
            ELSE 'Khác'
        END
    """


def _label_seller_type_sql(column: str = "seller_type") -> str:
    return f"""
        CASE
            WHEN {column} = 'official' THEN 'Official'
            WHEN {column} = 'marketplace' THEN 'Marketplace'
            WHEN {column} = 'mixed' THEN 'Đa loại'
            ELSE 'Không rõ'
        END
    """


def _backfill_dim_category_labels(cursor: Any, schema: str) -> None:
    cursor.execute(
        f"""
        UPDATE {schema}."dim_category"
        SET
            market_scope_label = {_label_market_scope_sql()},
            home_segment_label = {_label_home_segment_sql()},
            exclusion_reason_label = {_label_exclusion_reason_sql()},
            updated_at = CURRENT_TIMESTAMP
        """
    )


def _backfill_dim_seller_labels(cursor: Any, schema: str) -> None:
    cursor.execute(
        f"""
        UPDATE {schema}."dim_seller"
        SET
            seller_type_label = {_label_seller_type_sql()},
            updated_at = CURRENT_TIMESTAMP
        """
    )


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
            COALESCE(p.seller_id, p.seller_pk::text, 'unknown'),
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
    refreshed_rows = cursor.rowcount
    _backfill_dim_product_labels(cursor, schema)
    return refreshed_rows


def _backfill_dim_product_labels(cursor: Any, schema: str) -> None:
    cursor.execute(
        f"""
        UPDATE {schema}."dim_product"
        SET
            market_scope_label = {_label_market_scope_sql()},
            home_segment_label = {_label_home_segment_sql()},
            exclusion_reason_label = {_label_exclusion_reason_sql()},
            updated_at = CURRENT_TIMESTAMP
        """
    )


def _refresh_fact_product_daily(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        INSERT INTO {schema}."fact_product_daily" (
            product_id, snapshot_date, crawl_run_id, price, original_price,
            discount_percent, price_bucket, price_bucket_label, price_status,
            price_status_label, discount_bucket, discount_bucket_label, discount_status,
            discount_status_label,
            rating_average, rating_bucket, rating_bucket_label, rating_status,
            rating_status_label, review_count, review_bucket, review_bucket_label,
            review_status, review_status_label, sales_count, sold_bucket, sold_bucket_label,
            sold_status, sold_status_label,
            seller_id, category_id, market_scope, market_scope_label, home_segment,
            home_segment_label, is_analysis_target, value_score, market_position,
            market_position_label, recorded_at, updated_at
        )
        WITH latest_product_snapshot AS (
            SELECT DISTINCT ON (p.product_id, COALESCE(pp.recorded_at, p.crawled_at)::date)
                p.product_id,
                COALESCE(pp.recorded_at, p.crawled_at)::date AS snapshot_date,
                pp.crawl_run_id,
                COALESCE(pp.price, p.price) AS raw_price,
                COALESCE(pp.original_price, p.original_price) AS raw_original_price,
                COALESCE(pp.discount_rate, p.discount_percent) AS raw_discount_percent,
                COALESCE(pm.rating_average, p.rating_average) AS rating_average,
                COALESCE(pm.review_count, p.review_count) AS review_count,
                COALESCE(pm.quantity_sold, p.sales_count) AS sales_count,
                COALESCE(dp.seller_id, 'unknown') AS seller_id,
                dp.category_id,
                dp.market_scope,
                dp.home_segment,
                dp.is_analysis_target,
                COALESCE(pp.recorded_at, p.crawled_at) AS recorded_at
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
        ),
        normalized_snapshot AS (
            SELECT
                product_id,
                snapshot_date,
                crawl_run_id,
                raw_price,
                CASE WHEN raw_price > 0 THEN raw_price END AS price,
                CASE
                    WHEN raw_original_price >= raw_price THEN raw_original_price
                    ELSE NULL
                END AS original_price,
                CASE
                    WHEN raw_original_price > 0 AND raw_original_price >= raw_price THEN
                        ROUND((raw_original_price - raw_price) / raw_original_price * 100, 2)
                    WHEN raw_discount_percent >= 0 THEN raw_discount_percent
                    ELSE NULL
                END AS discount_percent,
                rating_average,
                review_count,
                sales_count,
                seller_id,
                category_id,
                market_scope,
                home_segment,
                is_analysis_target,
                recorded_at
            FROM latest_product_snapshot
        ),
        classified_snapshot AS (
            SELECT
                *,
                CASE
                    WHEN raw_price IS NULL THEN 'price_unknown'
                    WHEN raw_price = 0 THEN 'price_zero'
                    WHEN price < 100000 THEN 'low'
                    WHEN price < 500000 THEN 'mid'
                    WHEN price < 2000000 THEN 'high'
                    ELSE 'premium'
                END AS price_bucket,
                CASE
                    WHEN raw_price IS NULL THEN 'missing_price'
                    WHEN raw_price = 0 THEN 'zero_price'
                    ELSE 'has_price'
                END AS price_status,
                CASE
                    WHEN discount_percent IS NULL THEN 'discount_unknown'
                    WHEN discount_percent = 0 THEN 'no_discount'
                    WHEN discount_percent < 10 THEN 'low_discount'
                    WHEN discount_percent < 30 THEN 'mid_discount'
                    ELSE 'high_discount'
                END AS discount_bucket,
                CASE
                    WHEN discount_percent IS NULL THEN 'missing_discount'
                    WHEN discount_percent = 0 THEN 'zero_discount'
                    ELSE 'has_discount'
                END AS discount_status,
                CASE
                    WHEN rating_average IS NULL THEN 'rating_unknown'
                    WHEN rating_average = 0 THEN 'rating_zero'
                    WHEN rating_average < 3.5 THEN 'rating_low'
                    WHEN rating_average < 4.5 THEN 'rating_mid'
                    ELSE 'rating_high'
                END AS rating_bucket,
                CASE
                    WHEN rating_average IS NULL THEN 'missing_rating'
                    WHEN rating_average = 0 THEN 'zero_rating'
                    ELSE 'has_rating'
                END AS rating_status,
                CASE
                    WHEN review_count IS NULL THEN 'review_unknown'
                    WHEN review_count = 0 THEN 'no_review'
                    WHEN review_count < 10 THEN 'low_review'
                    WHEN review_count < 100 THEN 'moderate_review'
                    ELSE 'high_review'
                END AS review_bucket,
                CASE
                    WHEN review_count IS NULL THEN 'missing_review_count'
                    WHEN review_count = 0 THEN 'zero_review'
                    ELSE 'has_reviews'
                END AS review_status,
                CASE
                    WHEN sales_count IS NULL THEN 'sold_unknown'
                    WHEN sales_count = 0 THEN 'no_sold'
                    WHEN sales_count < 10 THEN 'low_sold'
                    WHEN sales_count < 100 THEN 'moderate_sold'
                    ELSE 'high_sold'
                END AS sold_bucket,
                CASE
                    WHEN sales_count IS NULL THEN 'missing_sales_count'
                    WHEN sales_count = 0 THEN 'zero_sold'
                    ELSE 'has_sales'
                END AS sold_status
            FROM normalized_snapshot
        )
        SELECT
            p.product_id,
            p.snapshot_date,
            p.crawl_run_id,
            p.price,
            p.original_price,
            p.discount_percent,
            p.price_bucket,
            CASE
                WHEN p.price_bucket = 'price_unknown' THEN 'Thiếu giá'
                WHEN p.price_bucket = 'price_zero' THEN 'Giá bằng 0'
                WHEN p.price_bucket = 'low' THEN 'Giá thấp'
                WHEN p.price_bucket = 'mid' THEN 'Giá TB'
                WHEN p.price_bucket = 'high' THEN 'Giá cao'
                ELSE 'Cao cấp'
            END AS price_bucket_label,
            p.price_status,
            CASE
                WHEN p.price_status = 'missing_price' THEN 'Thiếu giá'
                WHEN p.price_status = 'zero_price' THEN 'Giá 0'
                ELSE 'Có giá'
            END AS price_status_label,
            p.discount_bucket,
            CASE
                WHEN p.discount_bucket = 'discount_unknown' THEN 'Thiếu KM'
                WHEN p.discount_bucket = 'no_discount' THEN 'Không KM'
                WHEN p.discount_bucket = 'low_discount' THEN 'KM thấp'
                WHEN p.discount_bucket = 'mid_discount' THEN 'KM TB'
                ELSE 'KM cao'
            END AS discount_bucket_label,
            p.discount_status,
            CASE
                WHEN p.discount_status = 'missing_discount' THEN 'Thiếu KM'
                WHEN p.discount_status = 'zero_discount' THEN 'Không KM'
                ELSE 'Có KM'
            END AS discount_status_label,
            p.rating_average,
            p.rating_bucket,
            CASE
                WHEN p.rating_bucket = 'rating_unknown' THEN 'Thiếu rating'
                WHEN p.rating_bucket = 'rating_zero' THEN 'Rating 0'
                WHEN p.rating_bucket = 'rating_low' THEN 'Đánh giá thấp'
                WHEN p.rating_bucket = 'rating_mid' THEN 'Đánh giá TB'
                ELSE 'Đánh giá cao'
            END AS rating_bucket_label,
            p.rating_status,
            CASE
                WHEN p.rating_status = 'missing_rating' THEN 'Thiếu rating'
                WHEN p.rating_status = 'zero_rating' THEN 'Rating 0'
                ELSE 'Có rating'
            END AS rating_status_label,
            p.review_count,
            p.review_bucket,
            CASE
                WHEN p.review_bucket = 'review_unknown' THEN 'Thiếu review'
                WHEN p.review_bucket = 'no_review' THEN '0 review'
                WHEN p.review_bucket = 'low_review' THEN 'Ít review'
                WHEN p.review_bucket = 'moderate_review' THEN 'Review TB'
                ELSE 'Nhiều review'
            END AS review_bucket_label,
            p.review_status,
            CASE
                WHEN p.review_status = 'missing_review_count' THEN 'Thiếu review'
                WHEN p.review_status = 'zero_review' THEN '0 review'
                ELSE 'Có review'
            END AS review_status_label,
            p.sales_count,
            p.sold_bucket,
            CASE
                WHEN p.sold_bucket = 'sold_unknown' THEN 'Thiếu sold'
                WHEN p.sold_bucket = 'no_sold' THEN '0 sold'
                WHEN p.sold_bucket = 'low_sold' THEN 'Sold thấp'
                WHEN p.sold_bucket = 'moderate_sold' THEN 'Sold TB'
                ELSE 'Sold cao'
            END AS sold_bucket_label,
            p.sold_status,
            CASE
                WHEN p.sold_status = 'missing_sales_count' THEN 'Thiếu sold'
                WHEN p.sold_status = 'zero_sold' THEN '0 sold'
                ELSE 'Có sold'
            END AS sold_status_label,
            p.seller_id,
            p.category_id,
            p.market_scope,
            CASE
                WHEN p.market_scope = 'home_living' THEN 'Nhà cửa'
                WHEN p.market_scope = 'home_appliance' THEN 'Điện GD'
                WHEN p.market_scope = 'mixed' THEN 'Đa scope'
                ELSE 'Khác'
            END AS market_scope_label,
            p.home_segment,
            CASE
                WHEN p.home_segment = 'kitchen_cooking' THEN 'Bếp'
                WHEN p.home_segment = 'cleaning_homecare' THEN 'Vệ sinh'
                WHEN p.home_segment = 'laundry_garment' THEN 'Giặt ủi'
                WHEN p.home_segment = 'storage_organization' THEN 'Lưu trữ'
                WHEN p.home_segment = 'bathroom' THEN 'Nhà tắm'
                WHEN p.home_segment = 'home_comfort' THEN 'Tiện nghi'
                WHEN p.home_segment = 'decor_living' THEN 'Trang trí'
                ELSE 'Ngoài scope'
            END AS home_segment_label,
            p.is_analysis_target,
            CASE
                WHEN p.rating_average IS NULL OR p.review_count IS NULL THEN NULL
                ELSE (
                    p.rating_average * 20
                    + LEAST(GREATEST(p.review_count, 0), 1000) / 20.0
                    + COALESCE(p.discount_percent, 0) / 2.0
                )
            END AS value_score,
            CASE
                WHEN p.rating_average IS NULL OR p.review_count IS NULL THEN 'insufficient_data'
                WHEN p.price < 500000 AND p.review_count >= 100 THEN 'cheap_popular'
                WHEN p.price >= 500000 AND p.rating_average >= 4.5 THEN 'premium_quality'
                WHEN p.discount_percent >= 30 THEN 'high_discount'
                WHEN p.rating_average < 3.5 OR p.review_count < 10 THEN 'low_trust'
                ELSE 'normal'
            END AS market_position,
            CASE
                WHEN p.rating_average IS NULL OR p.review_count IS NULL THEN 'Thiếu tín hiệu'
                WHEN p.price < 500000 AND p.review_count >= 100 THEN 'Giá tốt'
                WHEN p.price >= 500000 AND p.rating_average >= 4.5 THEN 'Cao cấp tốt'
                WHEN p.discount_percent >= 30 THEN 'KM mạnh'
                WHEN p.rating_average < 3.5 OR p.review_count < 10 THEN 'Tin cậy thấp'
                ELSE 'Bình thường'
            END AS market_position_label,
            p.recorded_at,
            CURRENT_TIMESTAMP
        FROM classified_snapshot p
        ON CONFLICT (product_id, snapshot_date) DO UPDATE SET
            crawl_run_id = EXCLUDED.crawl_run_id,
            price = EXCLUDED.price,
            original_price = EXCLUDED.original_price,
            discount_percent = EXCLUDED.discount_percent,
            price_bucket = EXCLUDED.price_bucket,
            price_bucket_label = EXCLUDED.price_bucket_label,
            price_status = EXCLUDED.price_status,
            price_status_label = EXCLUDED.price_status_label,
            discount_bucket = EXCLUDED.discount_bucket,
            discount_bucket_label = EXCLUDED.discount_bucket_label,
            discount_status = EXCLUDED.discount_status,
            discount_status_label = EXCLUDED.discount_status_label,
            rating_average = EXCLUDED.rating_average,
            rating_bucket = EXCLUDED.rating_bucket,
            rating_bucket_label = EXCLUDED.rating_bucket_label,
            rating_status = EXCLUDED.rating_status,
            rating_status_label = EXCLUDED.rating_status_label,
            review_count = EXCLUDED.review_count,
            review_bucket = EXCLUDED.review_bucket,
            review_bucket_label = EXCLUDED.review_bucket_label,
            review_status = EXCLUDED.review_status,
            review_status_label = EXCLUDED.review_status_label,
            sales_count = EXCLUDED.sales_count,
            sold_bucket = EXCLUDED.sold_bucket,
            sold_bucket_label = EXCLUDED.sold_bucket_label,
            sold_status = EXCLUDED.sold_status,
            sold_status_label = EXCLUDED.sold_status_label,
            seller_id = EXCLUDED.seller_id,
            category_id = EXCLUDED.category_id,
            market_scope = EXCLUDED.market_scope,
            market_scope_label = EXCLUDED.market_scope_label,
            home_segment = EXCLUDED.home_segment,
            home_segment_label = EXCLUDED.home_segment_label,
            is_analysis_target = EXCLUDED.is_analysis_target,
            value_score = EXCLUDED.value_score,
            market_position = EXCLUDED.market_position,
            market_position_label = EXCLUDED.market_position_label,
            recorded_at = EXCLUDED.recorded_at,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    refreshed_rows = cursor.rowcount
    _backfill_fact_product_daily_labels(cursor, schema)
    return refreshed_rows


def _backfill_fact_product_daily_labels(cursor: Any, schema: str) -> None:
    cursor.execute(
        f"""
        UPDATE {schema}."fact_product_daily"
        SET
            price_status = COALESCE(
                price_status,
                CASE
                    WHEN price IS NULL THEN 'missing_price'
                    WHEN price = 0 THEN 'zero_price'
                    ELSE 'has_price'
                END
            ),
            price_status_label = CASE
                    WHEN COALESCE(price_status, '') = 'missing_price' THEN 'Thiếu giá'
                    WHEN COALESCE(price_status, '') = 'zero_price' THEN 'Giá 0'
                    WHEN price IS NULL THEN 'Thiếu giá'
                    WHEN price = 0 THEN 'Giá 0'
                    ELSE 'Có giá'
                END,
            discount_status = COALESCE(
                discount_status,
                CASE
                    WHEN discount_percent IS NULL THEN 'missing_discount'
                    WHEN discount_percent = 0 THEN 'zero_discount'
                    ELSE 'has_discount'
                END
            ),
            discount_status_label = CASE
                    WHEN COALESCE(discount_status, '') = 'missing_discount' THEN 'Thiếu KM'
                    WHEN COALESCE(discount_status, '') = 'zero_discount' THEN 'Không KM'
                    WHEN discount_percent IS NULL THEN 'Thiếu KM'
                    WHEN discount_percent = 0 THEN 'Không KM'
                    ELSE 'Có KM'
                END,
            rating_status = COALESCE(
                rating_status,
                CASE
                    WHEN rating_average IS NULL THEN 'missing_rating'
                    WHEN rating_average = 0 THEN 'zero_rating'
                    ELSE 'has_rating'
                END
            ),
            rating_status_label = CASE
                    WHEN COALESCE(rating_status, '') = 'missing_rating' THEN 'Thiếu rating'
                    WHEN COALESCE(rating_status, '') = 'zero_rating' THEN 'Rating 0'
                    WHEN rating_average IS NULL THEN 'Thiếu rating'
                    WHEN rating_average = 0 THEN 'Rating 0'
                    ELSE 'Có rating'
                END,
            review_status = COALESCE(
                review_status,
                CASE
                    WHEN review_count IS NULL THEN 'missing_review_count'
                    WHEN review_count = 0 THEN 'zero_review'
                    ELSE 'has_reviews'
                END
            ),
            review_status_label = CASE
                    WHEN COALESCE(review_status, '') = 'missing_review_count'
                        THEN 'Thiếu review'
                    WHEN COALESCE(review_status, '') = 'zero_review' THEN '0 review'
                    WHEN review_count IS NULL THEN 'Thiếu review'
                    WHEN review_count = 0 THEN '0 review'
                    ELSE 'Có review'
                END,
            sold_status = COALESCE(
                sold_status,
                CASE
                    WHEN sales_count IS NULL THEN 'missing_sales_count'
                    WHEN sales_count = 0 THEN 'zero_sold'
                    ELSE 'has_sales'
                END
            ),
            sold_status_label = CASE
                    WHEN COALESCE(sold_status, '') = 'missing_sales_count' THEN 'Thiếu sold'
                    WHEN COALESCE(sold_status, '') = 'zero_sold' THEN '0 sold'
                    WHEN sales_count IS NULL THEN 'Thiếu sold'
                    WHEN sales_count = 0 THEN '0 sold'
                    ELSE 'Có sold'
                END,
            price_bucket_label = CASE
                    WHEN price_bucket = 'price_unknown' THEN 'Thiếu giá'
                    WHEN price_bucket = 'price_zero' THEN 'Giá bằng 0'
                    WHEN price_bucket = 'low' THEN 'Giá thấp'
                    WHEN price_bucket = 'mid' THEN 'Giá TB'
                    WHEN price_bucket = 'high' THEN 'Giá cao'
                    WHEN price_bucket = 'premium' THEN 'Cao cấp'
                    ELSE 'Chưa phân nhóm giá'
                END,
            discount_bucket_label = CASE
                    WHEN discount_bucket = 'discount_unknown'
                        THEN 'Thiếu KM'
                    WHEN discount_bucket = 'no_discount' THEN 'Không KM'
                    WHEN discount_bucket = 'low_discount' THEN 'KM thấp'
                    WHEN discount_bucket = 'mid_discount' THEN 'KM TB'
                    WHEN discount_bucket = 'high_discount' THEN 'KM cao'
                    ELSE 'Chưa phân nhóm giảm giá'
                END,
            rating_bucket_label = CASE
                    WHEN rating_bucket = 'rating_unknown' THEN 'Thiếu rating'
                    WHEN rating_bucket = 'rating_zero' THEN 'Rating 0'
                    WHEN rating_bucket = 'rating_low' THEN 'Đánh giá thấp'
                    WHEN rating_bucket = 'rating_mid' THEN 'Đánh giá TB'
                    WHEN rating_bucket = 'rating_high' THEN 'Đánh giá cao'
                    ELSE 'Chưa phân nhóm đánh giá'
                END,
            review_bucket_label = CASE
                    WHEN review_bucket = 'review_unknown'
                        THEN 'Thiếu review'
                    WHEN review_bucket = 'no_review' THEN '0 review'
                    WHEN review_bucket = 'low_review' THEN 'Ít review'
                    WHEN review_bucket = 'moderate_review' THEN 'Review TB'
                    WHEN review_bucket = 'high_review' THEN 'Nhiều review'
                    ELSE 'Chưa phân nhóm lượt đánh giá'
                END,
            sold_bucket_label = CASE
                    WHEN sold_bucket = 'sold_unknown' THEN 'Thiếu sold'
                    WHEN sold_bucket = 'no_sold' THEN '0 sold'
                    WHEN sold_bucket = 'low_sold' THEN 'Sold thấp'
                    WHEN sold_bucket = 'moderate_sold' THEN 'Sold TB'
                    WHEN sold_bucket = 'high_sold' THEN 'Sold cao'
                    ELSE 'Chưa phân nhóm lượt bán'
                END,
            market_scope_label = CASE
                    WHEN market_scope = 'home_living' THEN 'Nhà cửa'
                    WHEN market_scope = 'home_appliance' THEN 'Điện GD'
                    WHEN market_scope = 'mixed' THEN 'Đa scope'
                    ELSE 'Khác'
                END,
            home_segment_label = CASE
                    WHEN home_segment = 'kitchen_cooking' THEN 'Bếp'
                    WHEN home_segment = 'cleaning_homecare'
                        THEN 'Vệ sinh'
                    WHEN home_segment = 'laundry_garment'
                        THEN 'Giặt ủi'
                    WHEN home_segment = 'storage_organization' THEN 'Lưu trữ'
                    WHEN home_segment = 'bathroom' THEN 'Nhà tắm'
                    WHEN home_segment = 'home_comfort' THEN 'Tiện nghi'
                    WHEN home_segment = 'decor_living' THEN 'Trang trí'
                    ELSE 'Ngoài scope'
                END,
            market_position_label = CASE
                    WHEN market_position = 'insufficient_data' THEN 'Thiếu tín hiệu'
                    WHEN market_position = 'cheap_popular' THEN 'Giá tốt'
                    WHEN market_position = 'premium_quality' THEN 'Cao cấp tốt'
                    WHEN market_position = 'high_discount' THEN 'KM mạnh'
                    WHEN market_position = 'low_trust' THEN 'Tin cậy thấp'
                    WHEN market_position = 'normal' THEN 'Bình thường'
                    ELSE 'Chưa phân loại vị trí'
                END,
            updated_at = CURRENT_TIMESTAMP
        """
    )


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
    refreshed_rows = cursor.rowcount
    _backfill_fact_category_daily_labels(cursor, schema)
    return refreshed_rows


def _backfill_fact_category_daily_labels(cursor: Any, schema: str) -> None:
    cursor.execute(
        f"""
        UPDATE {schema}."fact_category_daily"
        SET
            market_scope_label = {_label_market_scope_sql()},
            home_segment_label = {_label_home_segment_sql()},
            updated_at = CURRENT_TIMESTAMP
        """
    )


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
            MIN(COALESCE(ds.seller_type, 'unknown')),
            CASE
                WHEN COUNT(DISTINCT f.market_scope) > 1 THEN 'mixed'
                ELSE MIN(f.market_scope)
            END,
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
            f.home_segment
        """
    )
    refreshed_rows = cursor.rowcount
    _backfill_fact_seller_daily_labels(cursor, schema)
    return refreshed_rows


def _backfill_fact_seller_daily_labels(cursor: Any, schema: str) -> None:
    cursor.execute(
        f"""
        UPDATE {schema}."fact_seller_daily"
        SET
            seller_type_label = {_label_seller_type_sql()},
            market_scope_label = {_label_market_scope_sql()},
            home_segment_label = {_label_home_segment_sql()},
            updated_at = CURRENT_TIMESTAMP
        """
    )


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
    refreshed_rows = cursor.rowcount
    _backfill_fact_price_change_labels(cursor, schema)
    return refreshed_rows


def _backfill_fact_price_change_labels(cursor: Any, schema: str) -> None:
    cursor.execute(
        f"""
        UPDATE {schema}."fact_price_change"
        SET
            change_direction_label = CASE
                WHEN change_direction = 'increase' THEN 'Tăng'
                WHEN change_direction = 'decrease' THEN 'Giảm'
                WHEN change_direction = 'unchanged' THEN 'Không đổi'
                ELSE 'Không rõ'
            END,
            updated_at = CURRENT_TIMESTAMP
        """
    )


def _refresh_fact_crawl_quality(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        WITH raw_counts AS (
            SELECT crawl_run_id, COUNT(*) AS raw_product_count
            FROM raw_products
            WHERE crawl_run_id IS NOT NULL
            GROUP BY crawl_run_id
        ),
        task_counts AS (
            SELECT
                crawl_run_id,
                COUNT(*) AS task_count,
                COUNT(*) FILTER (WHERE status IN ('failed', 'error')) AS failed_task_count
            FROM crawl_tasks
            WHERE crawl_run_id IS NOT NULL
            GROUP BY crawl_run_id
        ),
        detail_quality AS (
            SELECT
                pp.crawl_run_id,
                COUNT(DISTINCT p.id) FILTER (WHERE p.detail_status IS NOT NULL)
                    AS detail_product_count,
                ROUND(AVG(p.data_quality_score), 2) AS avg_data_quality_score
            FROM product_prices pp
            JOIN products p ON p.id = pp.product_id
            WHERE pp.crawl_run_id IS NOT NULL
            GROUP BY pp.crawl_run_id
        )
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
            COALESCE(rc.raw_product_count, 0),
            COALESCE(tc.task_count, 0),
            COALESCE(tc.failed_task_count, 0),
            COALESCE(dq.detail_product_count, 0),
            dq.avg_data_quality_score,
            CURRENT_TIMESTAMP
        FROM crawl_runs cr
        LEFT JOIN raw_counts rc ON rc.crawl_run_id = cr.id
        LEFT JOIN task_counts tc ON tc.crawl_run_id = cr.id
        LEFT JOIN detail_quality dq ON dq.crawl_run_id = cr.id
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
    refreshed_rows = cursor.rowcount
    _backfill_fact_crawl_quality_labels(cursor, schema)
    return refreshed_rows


def _backfill_fact_crawl_quality_labels(cursor: Any, schema: str) -> None:
    cursor.execute(
        f"""
        UPDATE {schema}."fact_crawl_quality"
        SET
            status_label = CASE
                WHEN status = 'success' THEN 'Thành công'
                WHEN status = 'failed' THEN 'Thất bại'
                WHEN status = 'running' THEN 'Đang chạy'
                WHEN status = 'partial' THEN 'Một phần'
                ELSE 'Không rõ'
            END,
            updated_at = CURRENT_TIMESTAMP
        """
    )


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
