"""Build normalized analytics tables from the core Tiki product tables."""

from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from typing import Any

REQUIRED_ANALYTICS_TABLES = {
    "dim_snapshot",
    "dim_category",
    "dim_seller",
    "dim_product",
    "fact_product_snapshot",
    "fact_price_snapshot",
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
    """Classify a category path into stable technical taxonomy codes."""
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
    """Return idempotent DDL statements for normalized analytics tables."""
    schema = _q(schema_name)
    return [
        f"CREATE SCHEMA IF NOT EXISTS {schema}",
        *_drop_legacy_analytics_sql(schema),
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."dim_snapshot" (
            snapshot_id BIGSERIAL PRIMARY KEY,
            snapshot_at TIMESTAMP NOT NULL,
            source VARCHAR(50) NOT NULL DEFAULT 'core_current',
            crawl_run_id BIGINT,
            created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (source, snapshot_at)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."dim_category" (
            category_id VARCHAR(255) PRIMARY KEY,
            category_name VARCHAR(500) NOT NULL,
            category_path TEXT,
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
            seller_name VARCHAR(500) NOT NULL,
            seller_type VARCHAR(50) NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."dim_product" (
            product_id VARCHAR(255) PRIMARY KEY,
            product_name VARCHAR(1000) NOT NULL,
            short_name VARCHAR(255),
            brand VARCHAR(255),
            category_id VARCHAR(255) NOT NULL REFERENCES {schema}."dim_category"(category_id),
            seller_id VARCHAR(255) NOT NULL REFERENCES {schema}."dim_seller"(seller_id),
            product_url TEXT NOT NULL,
            image_url TEXT,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."fact_product_snapshot" (
            snapshot_id BIGINT NOT NULL REFERENCES {schema}."dim_snapshot"(snapshot_id),
            product_id VARCHAR(255) NOT NULL REFERENCES {schema}."dim_product"(product_id),
            category_id VARCHAR(255) NOT NULL REFERENCES {schema}."dim_category"(category_id),
            seller_id VARCHAR(255) NOT NULL REFERENCES {schema}."dim_seller"(seller_id),
            rating_average NUMERIC(4, 2),
            rating_bucket VARCHAR(50) NOT NULL,
            review_count INTEGER,
            review_bucket VARCHAR(50) NOT NULL,
            sales_count INTEGER,
            sold_bucket VARCHAR(50) NOT NULL,
            value_score NUMERIC(8, 2),
            market_position VARCHAR(80) NOT NULL,
            normalized_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (snapshot_id, product_id)
        )
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {schema}."fact_price_snapshot" (
            snapshot_id BIGINT NOT NULL REFERENCES {schema}."dim_snapshot"(snapshot_id),
            product_id VARCHAR(255) NOT NULL REFERENCES {schema}."dim_product"(product_id),
            price NUMERIC(12, 2) NOT NULL,
            original_price NUMERIC(12, 2),
            discount_percent NUMERIC(7, 2),
            price_bucket VARCHAR(50) NOT NULL,
            discount_bucket VARCHAR(50) NOT NULL,
            normalized_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (snapshot_id, product_id)
        )
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_fact_product_snapshot_product
            ON {schema}."fact_product_snapshot"(product_id)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_fact_product_snapshot_category
            ON {schema}."fact_product_snapshot"(category_id)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_fact_product_snapshot_seller
            ON {schema}."fact_product_snapshot"(seller_id)
        """,
        f"""
        CREATE INDEX IF NOT EXISTS idx_fact_price_snapshot_product
            ON {schema}."fact_price_snapshot"(product_id)
        """,
        *_build_analysis_view_sql(schema),
    ]


def _drop_legacy_analytics_sql(schema: str) -> list[str]:
    schema_name = schema.strip('"')
    return [
        f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM pg_class cls
                JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                WHERE ns.nspname = {schema_name!r}
                  AND cls.relname = 'product_analysis_ready'
                  AND cls.relkind IN ('r', 'p')
            ) THEN
                EXECUTE 'DROP TABLE {schema}."product_analysis_ready" CASCADE';
            ELSIF EXISTS (
                SELECT 1
                FROM pg_class cls
                JOIN pg_namespace ns ON ns.oid = cls.relnamespace
                WHERE ns.nspname = {schema_name!r}
                  AND cls.relname = 'product_analysis_ready'
                  AND cls.relkind = 'v'
            ) THEN
                EXECUTE 'DROP VIEW {schema}."product_analysis_ready" CASCADE';
            END IF;
        END $$;
        """,
        f'DROP VIEW IF EXISTS {schema}."product_analysis_ready" CASCADE',
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
        f'DROP TABLE IF EXISTS {schema}."seller_summary" CASCADE',
        f'DROP TABLE IF EXISTS {schema}."category_summary" CASCADE',
    ]


def _build_analysis_view_sql(schema: str) -> list[str]:
    return [
        f'DROP VIEW IF EXISTS {schema}."product_analysis_ready" CASCADE',
        f"""
        CREATE OR REPLACE VIEW {schema}."product_analysis_ready" AS
        SELECT
            ss.snapshot_id,
            ss.snapshot_at,
            p.product_id,
            p.product_name,
            p.short_name,
            p.brand,
            p.category_id,
            c.category_name,
            c.category_path,
            c.root_category_name,
            c.market_scope,
            c.home_segment,
            p.seller_id,
            s.seller_name,
            s.seller_type,
            price.price,
            price.original_price,
            price.discount_percent,
            price.price_bucket,
            price.discount_bucket,
            f.rating_average,
            f.rating_bucket,
            f.review_count,
            f.review_bucket,
            f.sales_count,
            f.sold_bucket,
            f.value_score,
            f.market_position,
            p.product_url,
            p.image_url,
            f.normalized_at
        FROM {schema}."fact_product_snapshot" f
        JOIN {schema}."dim_snapshot" ss ON ss.snapshot_id = f.snapshot_id
        JOIN {schema}."dim_product" p ON p.product_id = f.product_id
        JOIN {schema}."dim_category" c ON c.category_id = f.category_id
        JOIN {schema}."dim_seller" s ON s.seller_id = f.seller_id
        JOIN {schema}."fact_price_snapshot" price
            ON price.snapshot_id = f.snapshot_id
            AND price.product_id = f.product_id
        """,
    ]


def ensure_analytics_schema(cursor: Any, schema_name: str) -> dict[str, Any]:
    """Create normalized analytics tables and presentation view."""
    schema = validate_schema_name(schema_name)
    _execute_many(cursor, build_schema_sql(schema))
    return {"schema": schema, "tables": sorted(REQUIRED_ANALYTICS_TABLES)}


def refresh_analytics_mart(cursor: Any, schema_name: str) -> dict[str, Any]:
    """Refresh dimensions, snapshot metadata, and snapshot facts."""
    ensure_analytics_schema(cursor, schema_name)
    steps = {
        "dimensions": refresh_dimensions(cursor, schema_name),
        "dim_snapshot": refresh_snapshot(cursor, schema_name),
        "fact_product_snapshot": refresh_product_snapshot_fact(cursor, schema_name),
        "fact_price_snapshot": refresh_price_snapshot_fact(cursor, schema_name),
    }
    return {"schema": validate_schema_name(schema_name), "steps": steps}


def refresh_dimensions(cursor: Any, schema_name: str) -> dict[str, int]:
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {
        "dim_category": _refresh_dim_category(cursor, schema),
        "dim_seller": _refresh_dim_seller(cursor, schema),
        "dim_product": _refresh_dim_product(cursor, schema),
    }


def refresh_snapshot(cursor: Any, schema_name: str) -> dict[str, int]:
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {"dim_snapshot": _refresh_dim_snapshot(cursor, schema)}


def refresh_product_snapshot_fact(cursor: Any, schema_name: str) -> dict[str, int]:
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {"fact_product_snapshot": _refresh_fact_product_snapshot(cursor, schema)}


def refresh_price_snapshot_fact(cursor: Any, schema_name: str) -> dict[str, int]:
    schema = _q(schema_name)
    ensure_analytics_schema(cursor, schema_name)
    return {"fact_price_snapshot": _refresh_fact_price_snapshot(cursor, schema)}


def refresh_product_analysis_ready(cursor: Any, schema_name: str) -> dict[str, int]:
    """Compatibility wrapper: refresh every table behind the analysis view."""
    result = refresh_analytics_mart(cursor, schema_name)
    fact_rows = result["steps"]["fact_product_snapshot"]["fact_product_snapshot"]
    return {"product_analysis_ready": fact_rows}


def _execute_many(cursor: Any, statements: Iterable[str]) -> None:
    for statement in statements:
        cursor.execute(statement)


def _category_classification_sql() -> str:
    return """
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
        END
    """


def _refresh_dim_category(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        INSERT INTO {schema}."dim_category" (
            category_id, category_name, category_path, root_category_name,
            market_scope, home_segment, is_analysis_target, exclusion_reason, updated_at
        )
        WITH source AS (
            SELECT
                COALESCE(NULLIF(category_id, ''), NULLIF(tiki_category_id, '')) AS category_id,
                NULLIF(name, '') AS category_name,
                concat_ws(
                    ' > ',
                    NULLIF(level_1, ''),
                    NULLIF(level_2, ''),
                    NULLIF(level_3, ''),
                    NULLIF(level_4, ''),
                    NULLIF(level_5, '')
                ) AS category_path,
                NULLIF(root_category_name, '') AS root_category_name,
                lower(
                    concat_ws(
                        ' ',
                        level_1, level_2, level_3, level_4, level_5, root_category_name, name
                    )
                ) AS category_text
            FROM categories
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
                {_category_classification_sql()} AS home_segment,
                CASE
                    WHEN category_text LIKE '%nhạc cụ%'
                        OR category_text LIKE '%nhac cu%' THEN 'Nhạc cụ'
                    WHEN category_text LIKE '%đồ thờ%'
                        OR category_text LIKE '%do tho%' THEN 'Đồ thờ'
                    WHEN category_text LIKE '%sửa chữa nhà cửa%'
                        OR category_text LIKE '%sua chua nha cua%' THEN 'Sửa chữa'
                    WHEN category_text LIKE '%nội thất%'
                        OR category_text LIKE '%noi that%' THEN 'Nội thất lớn'
                    WHEN category_text LIKE '%máy giặt%'
                        OR category_text LIKE '%may giat%'
                        OR category_text LIKE '%tủ lạnh%'
                        OR category_text LIKE '%tu lanh%'
                        OR category_text LIKE '%máy lạnh%'
                        OR category_text LIKE '%may lanh%'
                        OR category_text LIKE '%máy điều hòa%'
                        OR category_text LIKE '%may dieu hoa%' THEN 'Điện máy lớn'
                    ELSE NULL
                END AS exclusion_reason
            FROM source
            WHERE category_id IS NOT NULL AND category_name IS NOT NULL
        )
        SELECT
            category_id,
            category_name,
            NULLIF(category_path, ''),
            root_category_name,
            market_scope,
            home_segment,
            home_segment <> 'Ngoài scope',
            CASE WHEN home_segment = 'Ngoài scope' THEN COALESCE(exclusion_reason, 'Chưa phân loại') END,
            CURRENT_TIMESTAMP
        FROM classified
        ON CONFLICT (category_id) DO UPDATE SET
            category_name = EXCLUDED.category_name,
            category_path = EXCLUDED.category_path,
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
            seller_id, seller_name, seller_type, updated_at
        )
        SELECT seller_id, seller_name, seller_type, CURRENT_TIMESTAMP
        FROM (
            SELECT DISTINCT ON (seller_id)
                seller_id,
                seller_name,
                seller_type
            FROM (
                SELECT
                    NULLIF(tiki_seller_id, '') AS seller_id,
                    NULLIF(name, '') AS seller_name,
                    CASE WHEN is_official IS TRUE THEN 'Official' ELSE 'Marketplace' END
                        AS seller_type,
                    1 AS source_priority
                FROM sellers
                UNION ALL
                SELECT
                    NULLIF(seller_id, '') AS seller_id,
                    NULLIF(seller_name, '') AS seller_name,
                    CASE WHEN seller_is_official IS TRUE THEN 'Official' ELSE 'Marketplace' END
                        AS seller_type,
                    2 AS source_priority
                FROM products
            ) candidates
            WHERE seller_id IS NOT NULL
              AND seller_name IS NOT NULL
            ORDER BY seller_id, source_priority
        ) source
        ON CONFLICT (seller_id) DO UPDATE SET
            seller_name = EXCLUDED.seller_name,
            seller_type = EXCLUDED.seller_type,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    return cursor.rowcount


def _refresh_dim_product(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        INSERT INTO {schema}."dim_product" (
            product_id, product_name, short_name, brand, category_id, seller_id,
            product_url, image_url, updated_at
        )
        WITH product_source AS (
            SELECT
                p.product_id,
                NULLIF(p.name, '') AS product_name,
                NULLIF(p.short_name, '') AS short_name,
                NULLIF(p.brand, '') AS brand,
                COALESCE(NULLIF(p.category_id, ''), NULLIF(c.category_id, '')) AS category_id,
                COALESCE(NULLIF(p.seller_id, ''), NULLIF(s.tiki_seller_id, '')) AS seller_id,
                NULLIF(p.url, '') AS product_url,
                p.image_url
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
        )
        SELECT
            ps.product_id,
            ps.product_name,
            ps.short_name,
            ps.brand,
            ps.category_id,
            ps.seller_id,
            ps.product_url,
            ps.image_url,
            CURRENT_TIMESTAMP
        FROM product_source ps
        JOIN {schema}."dim_category" c
            ON c.category_id = ps.category_id
            AND c.is_analysis_target IS TRUE
        JOIN {schema}."dim_seller" s ON s.seller_id = ps.seller_id
        WHERE ps.product_id IS NOT NULL
          AND ps.product_name IS NOT NULL
          AND ps.product_url IS NOT NULL
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name,
            short_name = EXCLUDED.short_name,
            brand = EXCLUDED.brand,
            category_id = EXCLUDED.category_id,
            seller_id = EXCLUDED.seller_id,
            product_url = EXCLUDED.product_url,
            image_url = EXCLUDED.image_url,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    return cursor.rowcount


def _refresh_dim_snapshot(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        INSERT INTO {schema}."dim_snapshot" (snapshot_at, source, crawl_run_id)
        SELECT
            COALESCE(MAX(COALESCE(p.crawled_at, p.updated_at)), CURRENT_TIMESTAMP),
            'core_current',
            NULL
        FROM products p
        JOIN {schema}."dim_product" dp ON dp.product_id = p.product_id
        ON CONFLICT (source, snapshot_at) DO UPDATE SET
            created_at = {schema}."dim_snapshot".created_at
        """
    )
    return cursor.rowcount


def _current_snapshot_cte(schema: str) -> str:
    return f"""
        current_snapshot AS (
            SELECT snapshot_id
            FROM {schema}."dim_snapshot"
            WHERE source = 'core_current'
            ORDER BY snapshot_at DESC, snapshot_id DESC
            LIMIT 1
        )
    """


def _refresh_fact_product_snapshot(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        WITH {_current_snapshot_cte(schema)}
        DELETE FROM {schema}."fact_product_snapshot" f
        USING current_snapshot cs
        WHERE f.snapshot_id = cs.snapshot_id
        """
    )
    cursor.execute(
        f"""
        INSERT INTO {schema}."fact_product_snapshot" (
            snapshot_id, product_id, category_id, seller_id,
            rating_average, rating_bucket, review_count, review_bucket,
            sales_count, sold_bucket, value_score, market_position,
            normalized_at, updated_at
        )
        WITH {_current_snapshot_cte(schema)}
        SELECT
            cs.snapshot_id,
            dp.product_id,
            dp.category_id,
            dp.seller_id,
            p.rating_average,
            CASE
                WHEN p.rating_average IS NULL THEN 'Thiếu rating'
                WHEN p.rating_average = 0 THEN 'Rating 0'
                WHEN p.rating_average < 3.5 THEN 'Đánh giá thấp'
                WHEN p.rating_average < 4.5 THEN 'Đánh giá TB'
                ELSE 'Đánh giá cao'
            END AS rating_bucket,
            p.review_count,
            CASE
                WHEN p.review_count IS NULL THEN 'Thiếu review'
                WHEN p.review_count = 0 THEN '0 review'
                WHEN p.review_count < 10 THEN 'Ít review'
                WHEN p.review_count < 100 THEN 'Review TB'
                ELSE 'Nhiều review'
            END AS review_bucket,
            p.sales_count,
            CASE
                WHEN p.sales_count IS NULL THEN 'Thiếu sold'
                WHEN p.sales_count = 0 THEN '0 sold'
                WHEN p.sales_count < 10 THEN 'Sold thấp'
                WHEN p.sales_count < 100 THEN 'Sold TB'
                ELSE 'Sold cao'
            END AS sold_bucket,
            CASE
                WHEN p.rating_average IS NULL OR p.review_count IS NULL THEN NULL
                ELSE ROUND(
                    p.rating_average * 20
                    + LEAST(GREATEST(p.review_count, 0), 1000) / 20.0
                    + COALESCE(p.discount_percent, 0) / 2.0,
                    2
                )
            END AS value_score,
            CASE
                WHEN p.rating_average IS NULL OR p.review_count IS NULL THEN 'Thiếu tín hiệu'
                WHEN p.price < 500000 AND p.review_count >= 100 THEN 'Giá tốt'
                WHEN p.price >= 500000 AND p.rating_average >= 4.5 THEN 'Cao cấp tốt'
                WHEN p.discount_percent >= 30 THEN 'KM mạnh'
                WHEN p.rating_average < 3.5 OR p.review_count < 10 THEN 'Tin cậy thấp'
                ELSE 'Bình thường'
            END AS market_position,
            COALESCE(p.crawled_at, p.updated_at, CURRENT_TIMESTAMP),
            CURRENT_TIMESTAMP
        FROM products p
        JOIN {schema}."dim_product" dp ON dp.product_id = p.product_id
        CROSS JOIN current_snapshot cs
        WHERE p.price > 0
        ON CONFLICT (snapshot_id, product_id) DO UPDATE SET
            category_id = EXCLUDED.category_id,
            seller_id = EXCLUDED.seller_id,
            rating_average = EXCLUDED.rating_average,
            rating_bucket = EXCLUDED.rating_bucket,
            review_count = EXCLUDED.review_count,
            review_bucket = EXCLUDED.review_bucket,
            sales_count = EXCLUDED.sales_count,
            sold_bucket = EXCLUDED.sold_bucket,
            value_score = EXCLUDED.value_score,
            market_position = EXCLUDED.market_position,
            normalized_at = EXCLUDED.normalized_at,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    return cursor.rowcount


def _refresh_fact_price_snapshot(cursor: Any, schema: str) -> int:
    cursor.execute(
        f"""
        WITH {_current_snapshot_cte(schema)}
        DELETE FROM {schema}."fact_price_snapshot" f
        USING current_snapshot cs
        WHERE f.snapshot_id = cs.snapshot_id
        """
    )
    cursor.execute(
        f"""
        INSERT INTO {schema}."fact_price_snapshot" (
            snapshot_id, product_id, price, original_price, discount_percent,
            price_bucket, discount_bucket, normalized_at, updated_at
        )
        WITH {_current_snapshot_cte(schema)}
        SELECT
            cs.snapshot_id,
            dp.product_id,
            p.price,
            CASE WHEN p.original_price >= p.price THEN p.original_price END AS original_price,
            CASE
                WHEN p.discount_percent >= 0 THEN p.discount_percent
                WHEN p.original_price > 0 AND p.original_price >= p.price THEN
                    ROUND((p.original_price - p.price) / p.original_price * 100, 2)
                ELSE NULL
            END AS discount_percent,
            CASE
                WHEN p.price < 100000 THEN 'Giá thấp'
                WHEN p.price < 500000 THEN 'Giá TB'
                WHEN p.price < 2000000 THEN 'Giá cao'
                ELSE 'Cao cấp'
            END AS price_bucket,
            CASE
                WHEN p.discount_percent IS NULL OR p.discount_percent = 0 THEN 'Không KM'
                WHEN p.discount_percent < 10 THEN 'KM thấp'
                WHEN p.discount_percent < 30 THEN 'KM TB'
                ELSE 'KM cao'
            END AS discount_bucket,
            COALESCE(p.crawled_at, p.updated_at, CURRENT_TIMESTAMP),
            CURRENT_TIMESTAMP
        FROM products p
        JOIN {schema}."dim_product" dp ON dp.product_id = p.product_id
        CROSS JOIN current_snapshot cs
        WHERE p.price > 0
        ON CONFLICT (snapshot_id, product_id) DO UPDATE SET
            price = EXCLUDED.price,
            original_price = EXCLUDED.original_price,
            discount_percent = EXCLUDED.discount_percent,
            price_bucket = EXCLUDED.price_bucket,
            discount_bucket = EXCLUDED.discount_bucket,
            normalized_at = EXCLUDED.normalized_at,
            updated_at = CURRENT_TIMESTAMP
        """
    )
    return cursor.rowcount
