"""Build a clean current-snapshot product analytics table."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from typing import Any

REQUIRED_ANALYTICS_TABLES = {
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
    """Return idempotent DDL statements for the clean analytics schema."""
    schema = _q(schema_name)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {schema}",
        *_drop_legacy_analytics_sql(schema),
        f"""
        CREATE TABLE {schema}."product_analysis_ready" (
            product_id VARCHAR(255) PRIMARY KEY,
            product_name VARCHAR(1000) NOT NULL,
            short_name VARCHAR(255),
            brand VARCHAR(255),
            category_id VARCHAR(255) NOT NULL,
            category_name VARCHAR(500) NOT NULL,
            category_path TEXT,
            root_category_name VARCHAR(255),
            market_scope VARCHAR(50) NOT NULL,
            home_segment VARCHAR(80) NOT NULL,
            seller_id VARCHAR(255) NOT NULL,
            seller_name VARCHAR(500) NOT NULL,
            seller_type VARCHAR(50) NOT NULL,
            price NUMERIC(12, 2) NOT NULL,
            original_price NUMERIC(12, 2),
            discount_percent NUMERIC(7, 2),
            price_bucket VARCHAR(50) NOT NULL,
            discount_bucket VARCHAR(50) NOT NULL,
            rating_average NUMERIC(4, 2),
            rating_bucket VARCHAR(50) NOT NULL,
            review_count INTEGER,
            review_bucket VARCHAR(50) NOT NULL,
            sales_count INTEGER,
            sold_bucket VARCHAR(50) NOT NULL,
            value_score NUMERIC(8, 2),
            market_position VARCHAR(80) NOT NULL,
            product_url TEXT NOT NULL,
            image_url TEXT,
            normalized_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_product_analysis_ready_segment
            ON {schema}."product_analysis_ready"(home_segment)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_product_analysis_ready_seller
            ON {schema}."product_analysis_ready"(seller_id)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_product_analysis_ready_category
            ON {schema}."product_analysis_ready"(category_id)
        """,
    ]


def _drop_legacy_analytics_sql(schema: str) -> list[str]:
    return [
        f'DROP VIEW IF EXISTS {schema}."mart_crawl_quality" CASCADE',
        f'DROP VIEW IF EXISTS {schema}."mart_seller_summary" CASCADE',
        f'DROP VIEW IF EXISTS {schema}."mart_category_summary" CASCADE',
        f'DROP VIEW IF EXISTS {schema}."mart_product_wide" CASCADE',
        f'DROP VIEW IF EXISTS {schema}."mart_product_analysis_ready" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."fact_crawl_quality" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."fact_price_change" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."fact_seller_daily" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."fact_category_daily" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."fact_product_daily" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."dim_product" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."dim_seller" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."dim_category" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."seller_summary" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."category_summary" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."product_analysis_ready" CASCADE',
    ]


def ensure_analytics_schema(cursor: Any, schema_name: str) -> dict[str, Any]:
    """Recreate the derived analytics table."""
    schema = validate_schema_name(schema_name)
    _execute_many(cursor, build_schema_sql(schema))
    return {"schema": schema, "tables": sorted(REQUIRED_ANALYTICS_TABLES)}


def refresh_analytics_mart(cursor: Any, schema_name: str) -> dict[str, Any]:
    """Refresh the full current-snapshot analytics table."""
    ensure_analytics_schema(cursor, schema_name)
    steps = {"product_analysis_ready": refresh_product_analysis_ready(cursor, schema_name)}
    return {"schema": validate_schema_name(schema_name), "steps": steps}


def refresh_product_analysis_ready(cursor: Any, schema_name: str) -> dict[str, int]:
    """Refresh the single clean table used by product analytics dashboards."""
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {"product_analysis_ready": _refresh_product_analysis_ready(cursor, schema)}


def _execute_many(cursor: Any, statements: Iterable[str]) -> None:
    for statement in statements:
        cursor.execute(statement)


def _refresh_product_analysis_ready(cursor: Any, schema: str) -> int:
    cursor.execute(f'DELETE FROM {schema}."product_analysis_ready"')
    cursor.execute(
        f"""
        INSERT INTO {schema}."product_analysis_ready" (
            product_id, product_name, short_name, brand,
            category_id, category_name, category_path, root_category_name,
            market_scope, home_segment,
            seller_id, seller_name, seller_type,
            price, original_price, discount_percent,
            price_bucket, discount_bucket,
            rating_average, rating_bucket,
            review_count, review_bucket,
            sales_count, sold_bucket,
            value_score, market_position,
            product_url, image_url, normalized_at
        )
        WITH source AS (
            SELECT
                p.product_id,
                NULLIF(p.name, '') AS product_name,
                NULLIF(p.short_name, '') AS short_name,
                NULLIF(p.brand, '') AS brand,
                COALESCE(NULLIF(p.category_id, ''), NULLIF(c.category_id, '')) AS category_id,
                NULLIF(c.name, '') AS category_name,
                concat_ws(
                    ' > ',
                    NULLIF(c.level_1, ''),
                    NULLIF(c.level_2, ''),
                    NULLIF(c.level_3, ''),
                    NULLIF(c.level_4, ''),
                    NULLIF(c.level_5, '')
                ) AS category_path,
                NULLIF(c.root_category_name, '') AS root_category_name,
                COALESCE(NULLIF(p.seller_id, ''), NULLIF(s.tiki_seller_id, '')) AS seller_id,
                COALESCE(NULLIF(p.seller_name, ''), NULLIF(s.name, '')) AS seller_name,
                CASE
                    WHEN COALESCE(p.seller_is_official, s.is_official) IS TRUE THEN 'Official'
                    ELSE 'Marketplace'
                END AS seller_type,
                p.price,
                CASE WHEN p.original_price >= p.price THEN p.original_price END AS original_price,
                CASE
                    WHEN p.discount_percent >= 0 THEN p.discount_percent
                    WHEN p.original_price > 0 AND p.original_price >= p.price THEN
                        ROUND((p.original_price - p.price) / p.original_price * 100, 2)
                    ELSE NULL
                END AS discount_percent,
                p.rating_average,
                p.review_count,
                p.sales_count,
                p.url AS product_url,
                p.image_url,
                COALESCE(p.crawled_at, p.updated_at, CURRENT_TIMESTAMP) AS normalized_at,
                lower(
                    concat_ws(
                        ' ',
                        c.level_1,
                        c.level_2,
                        c.level_3,
                        c.level_4,
                        c.level_5,
                        c.root_category_name,
                        c.name
                    )
                ) AS category_text
            FROM products p
            LEFT JOIN LATERAL (
                SELECT c.*
                FROM categories c
                WHERE c.category_id = p.category_id
                   OR c.tiki_category_id = p.category_id
                ORDER BY c.id
                LIMIT 1
            ) c ON TRUE
            LEFT JOIN LATERAL (
                SELECT s.*
                FROM sellers s
                WHERE s.id = p.seller_pk
                   OR s.tiki_seller_id = p.seller_id
                ORDER BY s.id
                LIMIT 1
            ) s ON TRUE
            WHERE p.product_id IS NOT NULL
        ),
        classified AS (
            SELECT
                *,
                CASE
                    WHEN category_text LIKE '%nhà cửa đời sống%'
                        OR category_text LIKE '%nha cua doi song%'
                        OR category_text LIKE '%nhà bếp%'
                        OR category_text LIKE '%nha bep%'
                        OR category_text LIKE '%phòng ăn%'
                        OR category_text LIKE '%phong an%'
                    THEN 'Nhà cửa'
                    WHEN category_text LIKE '%điện gia dụng%'
                        OR category_text LIKE '%dien gia dung%'
                        OR category_text LIKE '%điện tử điện lạnh%'
                        OR category_text LIKE '%dien tu dien lanh%'
                    THEN 'Điện GD'
                    ELSE 'Khác'
                END AS market_scope,
                CASE
                    WHEN category_text LIKE '%nhạc cụ%'
                        OR category_text LIKE '%nhac cu%'
                        OR category_text LIKE '%đồ thờ%'
                        OR category_text LIKE '%do tho%'
                        OR category_text LIKE '%sửa chữa nhà cửa%'
                        OR category_text LIKE '%sua chua nha cua%'
                        OR category_text LIKE '%nội thất%'
                        OR category_text LIKE '%noi that%'
                        OR category_text LIKE '%máy giặt%'
                        OR category_text LIKE '%may giat%'
                        OR category_text LIKE '%tủ lạnh%'
                        OR category_text LIKE '%tu lanh%'
                        OR category_text LIKE '%máy lạnh%'
                        OR category_text LIKE '%may lanh%'
                        OR category_text LIKE '%máy điều hòa%'
                        OR category_text LIKE '%may dieu hoa%'
                    THEN 'Ngoài scope'
                    WHEN category_text LIKE '%dụng cụ nhà bếp%'
                        OR category_text LIKE '%dung cu nha bep%'
                        OR category_text LIKE '%đồ dùng phòng ăn%'
                        OR category_text LIKE '%do dung phong an%'
                        OR category_text LIKE '%bình đựng nước%'
                        OR category_text LIKE '%binh dung nuoc%'
                    THEN 'Bếp'
                    WHEN category_text LIKE '%vệ sinh%'
                        OR category_text LIKE '%ve sinh%'
                        OR category_text LIKE '%dọn dẹp%'
                        OR category_text LIKE '%don dep%'
                        OR category_text LIKE '%hút bụi%'
                        OR category_text LIKE '%hut bui%'
                        OR category_text LIKE '%lọc không khí%'
                        OR category_text LIKE '%loc khong khi%'
                    THEN 'Vệ sinh'
                    WHEN category_text LIKE '%giặt%'
                        OR category_text LIKE '%giat%'
                        OR category_text LIKE '%ủi%'
                        OR category_text LIKE '% ui %'
                        OR category_text LIKE '%sấy quần áo%'
                        OR category_text LIKE '%say quan ao%'
                    THEN 'Giặt ủi'
                    WHEN category_text LIKE '%giá, móc%'
                        OR category_text LIKE '%gia, moc%'
                        OR category_text LIKE '%cây treo%'
                        OR category_text LIKE '%cay treo%'
                        OR category_text LIKE '%hộp đựng%'
                        OR category_text LIKE '%hop dung%'
                        OR category_text LIKE '%lưu trữ%'
                        OR category_text LIKE '%luu tru%'
                    THEN 'Lưu trữ'
                    WHEN category_text LIKE '%nhà tắm%'
                        OR category_text LIKE '%nha tam%'
                        OR category_text LIKE '%phòng tắm%'
                        OR category_text LIKE '%phong tam%'
                    THEN 'Nhà tắm'
                    WHEN category_text LIKE '%máy nước nóng%'
                        OR category_text LIKE '%may nuoc nong%'
                        OR category_text LIKE '%máy rửa chén%'
                        OR category_text LIKE '%may rua chen%'
                        OR category_text LIKE '%quạt%'
                        OR category_text LIKE '%quat%'
                        OR category_text LIKE '%tạo ẩm%'
                        OR category_text LIKE '%tao am%'
                        OR category_text LIKE '%máy sưởi%'
                        OR category_text LIKE '%may suoi%'
                    THEN 'Tiện nghi'
                    WHEN category_text LIKE '%trang trí%'
                        OR category_text LIKE '%trang tri%'
                        OR category_text LIKE '%đèn%'
                        OR category_text LIKE '%den%'
                        OR category_text LIKE '%chiếu sáng%'
                        OR category_text LIKE '%chieu sang%'
                    THEN 'Trang trí'
                    ELSE 'Ngoài scope'
                END AS home_segment
            FROM source
        ),
        clean AS (
            SELECT *
            FROM classified
            WHERE product_name IS NOT NULL
              AND product_url IS NOT NULL
              AND price > 0
              AND category_id IS NOT NULL
              AND category_name IS NOT NULL
              AND seller_id IS NOT NULL
              AND seller_name IS NOT NULL
              AND home_segment <> 'Ngoài scope'
        )
        SELECT
            product_id,
            product_name,
            short_name,
            brand,
            category_id,
            category_name,
            NULLIF(category_path, '') AS category_path,
            root_category_name,
            market_scope,
            home_segment,
            seller_id,
            seller_name,
            seller_type,
            price,
            original_price,
            discount_percent,
            CASE
                WHEN price < 100000 THEN 'Giá thấp'
                WHEN price < 500000 THEN 'Giá TB'
                WHEN price < 2000000 THEN 'Giá cao'
                ELSE 'Cao cấp'
            END AS price_bucket,
            CASE
                WHEN discount_percent IS NULL OR discount_percent = 0 THEN 'Không KM'
                WHEN discount_percent < 10 THEN 'KM thấp'
                WHEN discount_percent < 30 THEN 'KM TB'
                ELSE 'KM cao'
            END AS discount_bucket,
            rating_average,
            CASE
                WHEN rating_average IS NULL THEN 'Thiếu rating'
                WHEN rating_average = 0 THEN 'Rating 0'
                WHEN rating_average < 3.5 THEN 'Đánh giá thấp'
                WHEN rating_average < 4.5 THEN 'Đánh giá TB'
                ELSE 'Đánh giá cao'
            END AS rating_bucket,
            review_count,
            CASE
                WHEN review_count IS NULL THEN 'Thiếu review'
                WHEN review_count = 0 THEN '0 review'
                WHEN review_count < 10 THEN 'Ít review'
                WHEN review_count < 100 THEN 'Review TB'
                ELSE 'Nhiều review'
            END AS review_bucket,
            sales_count,
            CASE
                WHEN sales_count IS NULL THEN 'Thiếu sold'
                WHEN sales_count = 0 THEN '0 sold'
                WHEN sales_count < 10 THEN 'Sold thấp'
                WHEN sales_count < 100 THEN 'Sold TB'
                ELSE 'Sold cao'
            END AS sold_bucket,
            CASE
                WHEN rating_average IS NULL OR review_count IS NULL THEN NULL
                ELSE ROUND(
                    rating_average * 20
                    + LEAST(GREATEST(review_count, 0), 1000) / 20.0
                    + COALESCE(discount_percent, 0) / 2.0,
                    2
                )
            END AS value_score,
            CASE
                WHEN rating_average IS NULL OR review_count IS NULL THEN 'Thiếu tín hiệu'
                WHEN price < 500000 AND review_count >= 100 THEN 'Giá tốt'
                WHEN price >= 500000 AND rating_average >= 4.5 THEN 'Cao cấp tốt'
                WHEN discount_percent >= 30 THEN 'KM mạnh'
                WHEN rating_average < 3.5 OR review_count < 10 THEN 'Tin cậy thấp'
                ELSE 'Bình thường'
            END AS market_position,
            product_url,
            image_url,
            normalized_at
        FROM clean
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            short_name = EXCLUDED.short_name,
            brand = EXCLUDED.brand,
            category_id = EXCLUDED.category_id,
            category_name = EXCLUDED.category_name,
            category_path = EXCLUDED.category_path,
            root_category_name = EXCLUDED.root_category_name,
            market_scope = EXCLUDED.market_scope,
            home_segment = EXCLUDED.home_segment,
            seller_id = EXCLUDED.seller_id,
            seller_name = EXCLUDED.seller_name,
            seller_type = EXCLUDED.seller_type,
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
            value_score = EXCLUDED.value_score,
            market_position = EXCLUDED.market_position,
            product_url = EXCLUDED.product_url,
            image_url = EXCLUDED.image_url,
            normalized_at = EXCLUDED.normalized_at
        """
    )
    return cursor.rowcount
