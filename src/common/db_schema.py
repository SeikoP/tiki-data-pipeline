"""Idempotent PostgreSQL schema guards for the crawl pipeline."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

logger = logging.getLogger(__name__)


PRODUCT_COLUMNS = {
    "short_name": "VARCHAR(255)",
    "image_url": "TEXT",
    "category_url": "TEXT",
    "category_id": "VARCHAR(255)",
    "sales_count": "INTEGER",
    "original_price": "DECIMAL(12, 2)",
    "discount_percent": "DECIMAL(5, 2)",
    "rating_average": "DECIMAL(4, 2)",
    "review_count": "INTEGER DEFAULT 0",
    "seller_name": "VARCHAR(500)",
    "seller_id": "VARCHAR(255)",
    "seller_is_official": "BOOLEAN DEFAULT FALSE",
    "brand": "VARCHAR(255)",
    "stock_available": "BOOLEAN DEFAULT TRUE",
    "shipping": "JSONB",
    "crawl_quality": "VARCHAR(50) DEFAULT 'basic'",
    "needs_enrichment": "BOOLEAN DEFAULT TRUE",
    "crawled_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
    "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}

CATEGORY_COLUMNS = {
    "category_id": "VARCHAR(255)",
    "image_url": "TEXT",
    "parent_url": "TEXT",
    "level": "INTEGER",
    "category_path": "JSONB",
    "level_1": "VARCHAR(255)",
    "level_2": "VARCHAR(255)",
    "level_3": "VARCHAR(255)",
    "level_4": "VARCHAR(255)",
    "level_5": "VARCHAR(255)",
    "root_category_name": "VARCHAR(255)",
    "is_leaf": "BOOLEAN DEFAULT FALSE",
    "product_count": "INTEGER DEFAULT 0",
    "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}

CRAWL_HISTORY_COLUMNS = {
    "discount_amount": "DECIMAL(12, 2)",
    "price_change": "DECIMAL(12, 2)",
    "price_change_percent": "DECIMAL(5, 2)",
    "previous_price": "DECIMAL(12, 2)",
    "previous_original_price": "DECIMAL(12, 2)",
    "previous_discount_percent": "DECIMAL(5, 2)",
    "sales_count": "INTEGER",
    "sales_change": "INTEGER",
    "rating_average": "DECIMAL(4, 2)",
    "review_count": "INTEGER",
    "is_flash_sale": "BOOLEAN DEFAULT FALSE",
    "crawl_type": "VARCHAR(20) DEFAULT 'price_change'",
    "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
}

INDEX_STATEMENTS = [
    "CREATE INDEX IF NOT EXISTS idx_products_category_url ON products(category_url)",
    "CREATE INDEX IF NOT EXISTS idx_products_category_id ON products(category_id)",
    "CREATE INDEX IF NOT EXISTS idx_products_sales_count ON products(sales_count)",
    "CREATE INDEX IF NOT EXISTS idx_products_crawled_at ON products(crawled_at)",
    (
        "CREATE INDEX IF NOT EXISTS idx_products_needs_enrichment "
        "ON products(needs_enrichment) WHERE needs_enrichment = true"
    ),
    "CREATE INDEX IF NOT EXISTS idx_categories_parent_url ON categories(parent_url)",
    "CREATE INDEX IF NOT EXISTS idx_categories_level ON categories(level)",
    "CREATE INDEX IF NOT EXISTS idx_categories_path ON categories USING GIN (category_path)",
    "CREATE INDEX IF NOT EXISTS idx_categories_is_leaf ON categories(is_leaf)",
    "CREATE INDEX IF NOT EXISTS idx_history_product_id ON crawl_history(product_id)",
    "CREATE INDEX IF NOT EXISTS idx_history_crawled_at ON crawl_history(crawled_at)",
    "CREATE INDEX IF NOT EXISTS idx_history_product_latest ON crawl_history(product_id, crawled_at DESC)",
]


def _table_exists(cursor: Any, table_name: str) -> bool:
    cursor.execute(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
        )
        """,
        (table_name,),
    )
    row = cursor.fetchone()
    return bool(row and row[0])


def _existing_columns(cursor: Any, table_name: str) -> set[str]:
    cursor.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table_name,),
    )
    return {row[0] for row in cursor.fetchall()}


def _ensure_columns(cursor: Any, table_name: str, columns: dict[str, str]) -> list[str]:
    if not _table_exists(cursor, table_name):
        raise RuntimeError(f"Required table does not exist: {table_name}")

    existing = _existing_columns(cursor, table_name)
    added: list[str] = []
    for column_name, column_type in columns.items():
        if column_name in existing:
            continue
        cursor.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")
        added.append(f"{table_name}.{column_name}")
    return added


def _execute_indexes(cursor: Any, statements: Iterable[str]) -> None:
    for statement in statements:
        try:
            cursor.execute(statement)
        except Exception as exc:  # noqa: BLE001 - indexes are best-effort schema hardening
            logger.warning("Could not ensure index with statement %r: %s", statement, exc)


def ensure_crawl_schema(cursor: Any) -> dict[str, Any]:
    """Ensure additive crawl schema fields used by current loaders exist.

    This intentionally only adds missing nullable/defaulted columns and indexes. It never drops,
    rewrites, or backfills data.
    """
    added_columns: list[str] = []
    added_columns.extend(_ensure_columns(cursor, "categories", CATEGORY_COLUMNS))
    added_columns.extend(_ensure_columns(cursor, "products", PRODUCT_COLUMNS))
    added_columns.extend(_ensure_columns(cursor, "crawl_history", CRAWL_HISTORY_COLUMNS))
    _execute_indexes(cursor, INDEX_STATEMENTS)

    if added_columns:
        logger.info("Ensured crawl schema; added columns: %s", ", ".join(added_columns))
    return {"status": "success", "added_columns": added_columns}


__all__ = ["ensure_crawl_schema"]
