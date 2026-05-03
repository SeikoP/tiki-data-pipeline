"""Run, raw, and snapshot persistence for the layered crawl pipeline."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from psycopg2.extras import Json, execute_values

from common.db_schema import ensure_crawl_schema


def _cursor():
    from pipelines.load.db_pool import get_db_pool, initialize_db_pool

    initialize_db_pool()
    return get_db_pool().get_cursor(commit=True)


def _raw_category_id(product: dict[str, Any]) -> str | None:
    try:
        from common.category_utils import normalize_category_id

        return normalize_category_id(product.get("category_id"), product.get("category_url"))
    except Exception:
        category_id = product.get("category_id")
        if category_id:
            return str(category_id)
        return None


def start_crawl_run(
    *, airflow_dag_id: str | None, airflow_run_id: str | None, source: str = "tiki"
) -> int:
    """Create or resume a crawl run row for the current Airflow run."""
    with _cursor() as cur:
        ensure_crawl_schema(cur)
        cur.execute(
            """
            INSERT INTO crawl_runs (source, airflow_dag_id, airflow_run_id, status, started_at)
            VALUES (%s, %s, %s, 'running', CURRENT_TIMESTAMP)
            ON CONFLICT (airflow_dag_id, airflow_run_id) DO UPDATE SET
                status = 'running',
                error_message = NULL,
                started_at = COALESCE(crawl_runs.started_at, EXCLUDED.started_at),
                finished_at = NULL
            RETURNING id
            """,
            (source, airflow_dag_id, airflow_run_id),
        )
        return int(cur.fetchone()[0])


def finish_crawl_run(
    *,
    crawl_run_id: int,
    status: str,
    total_raw_items: int = 0,
    total_transformed_items: int = 0,
    total_loaded_items: int = 0,
    error_message: str | None = None,
) -> None:
    """Mark a crawl run as finished."""
    with _cursor() as cur:
        cur.execute(
            """
            UPDATE crawl_runs
            SET status = %s,
                finished_at = CURRENT_TIMESTAMP,
                total_raw_items = %s,
                total_transformed_items = %s,
                total_loaded_items = %s,
                error_message = %s
            WHERE id = %s
            """,
            (
                status,
                total_raw_items,
                total_transformed_items,
                total_loaded_items,
                error_message,
                crawl_run_id,
            ),
        )


def upsert_crawl_task(
    *,
    crawl_run_id: int,
    task_key: str,
    task_type: str,
    status: str,
    url: str | None = None,
    category_id: str | None = None,
    page: int | None = None,
    retry_count: int = 0,
    error_message: str | None = None,
) -> None:
    with _cursor() as cur:
        cur.execute(
            """
            INSERT INTO crawl_tasks (
                crawl_run_id, task_key, task_type, url, category_id,
                status, retry_count, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (crawl_run_id, task_key) DO UPDATE SET
                task_type = EXCLUDED.task_type,
                url = EXCLUDED.url,
                category_id = EXCLUDED.category_id,
                status = EXCLUDED.status,
                retry_count = EXCLUDED.retry_count,
                error_message = EXCLUDED.error_message
            """,
            (
                crawl_run_id,
                task_key,
                task_type,
                url,
                category_id,
                status,
                retry_count,
                error_message,
            ),
        )


def upsert_crawl_tasks(tasks: list[dict[str, Any]]) -> int:
    if not tasks:
        return 0

    rows = []
    for task in tasks:
        rows.append(
            (
                task["crawl_run_id"],
                task["task_key"],
                task["task_type"],
                task.get("url"),
                task.get("category_id"),
                task.get("status"),
                task.get("retry_count", 0),
                task.get("error_message"),
            )
        )

    with _cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO crawl_tasks (
                crawl_run_id, task_key, task_type, url, category_id,
                status, retry_count, error_message
            )
            VALUES %s
            ON CONFLICT (crawl_run_id, task_key) DO UPDATE SET
                task_type = EXCLUDED.task_type,
                url = EXCLUDED.url,
                category_id = EXCLUDED.category_id,
                status = EXCLUDED.status,
                retry_count = EXCLUDED.retry_count,
                error_message = EXCLUDED.error_message
            """,
            rows,
            page_size=500,
        )
    return len(rows)


def save_raw_products(
    products: list[dict[str, Any]], *, crawl_run_id: int | None, source_stage: str
) -> int:
    """Persist raw Tiki payloads for reprocessing/debugging."""
    if not crawl_run_id or not products:
        return 0

    now = datetime.now()
    rows = []
    for product in products:
        product_id = product.get("product_id") or product.get("id")
        if not product_id:
            continue
        rows.append(
            (
                crawl_run_id,
                str(product_id),
                source_stage,
                _raw_category_id(product),
                Json(product),
                product.get("crawled_at") or now,
            )
        )

    if not rows:
        return 0

    with _cursor() as cur:
        execute_values(
            cur,
            """
            INSERT INTO raw_products (
                crawl_run_id, tiki_product_id, source_stage, category_id, raw_json, crawled_at
            )
            VALUES %s
            ON CONFLICT (crawl_run_id, tiki_product_id, source_stage) DO UPDATE SET
                category_id = EXCLUDED.category_id,
                raw_json = EXCLUDED.raw_json,
                crawled_at = EXCLUDED.crawled_at
            """,
            rows,
            page_size=500,
        )
    return len(rows)


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _changed(current: Any, previous: Any) -> bool:
    return current != previous


def _write_crawl_history_events(
    cursor: Any,
    products: list[dict[str, Any]],
    product_pk_by_id: dict[str, int],
    crawl_run_id: int | None,
    now: datetime,
) -> int:
    if not crawl_run_id or not products:
        return 0

    product_pks = list(product_pk_by_id.values())
    if not product_pks:
        return 0

    cursor.execute(
        """
        WITH previous_price AS (
            SELECT DISTINCT ON (pp.product_id)
                pp.product_id,
                pp.price,
                pp.original_price,
                pp.discount_rate,
                pp.recorded_at,
                pp.crawl_run_id
            FROM product_prices pp
            WHERE pp.product_id = ANY(%s)
              AND pp.crawl_run_id IS DISTINCT FROM %s
            ORDER BY pp.product_id, pp.recorded_at DESC, pp.id DESC
        )
        SELECT
            p.product_id,
            prev.price,
            prev.original_price,
            prev.discount_rate,
            pm.rating_average,
            pm.review_count,
            pm.quantity_sold
        FROM previous_price prev
        JOIN products p ON p.id = prev.product_id
        LEFT JOIN product_metrics pm
          ON pm.product_id = prev.product_id
         AND pm.crawl_run_id IS NOT DISTINCT FROM prev.crawl_run_id
        """,
        (product_pks, crawl_run_id),
    )
    previous_by_product_id = {
        row[0]: {
            "price": row[1],
            "original_price": row[2],
            "discount_percent": row[3],
            "rating_average": row[4],
            "review_count": row[5],
            "sales_count": row[6],
        }
        for row in cursor.fetchall()
    }

    rows = []
    for product in products:
        product_id = str(product.get("product_id") or "")
        if not product_id or product_id not in product_pk_by_id:
            continue

        current_price = product.get("price")
        current_original = product.get("original_price")
        current_discount = product.get("discount_percent")
        current_sales = product.get("sales_count")
        current_rating = product.get("rating_average")
        current_reviews = product.get("review_count")

        prev = previous_by_product_id.get(product_id)
        prev_price = prev.get("price") if prev else None
        prev_original = prev.get("original_price") if prev else None
        prev_discount = prev.get("discount_percent") if prev else None
        prev_sales = prev.get("sales_count") if prev else None

        price_changed = prev is None or _changed(current_price, prev_price)
        original_changed = prev is None or _changed(current_original, prev_original)
        discount_changed = prev is None or _changed(current_discount, prev_discount)
        rating_changed = prev is None or _changed(current_rating, prev.get("rating_average"))
        reviews_changed = prev is None or _changed(current_reviews, prev.get("review_count"))

        sales_change = None
        sales_changed = False
        if prev_sales is not None and current_sales is not None:
            sales_change = current_sales - prev_sales
            sales_changed = sales_change != 0
        elif prev is None:
            sales_changed = current_sales is not None

        if not (
            price_changed
            or original_changed
            or discount_changed
            or rating_changed
            or reviews_changed
            or sales_changed
        ):
            continue

        current_price_float = _to_float(current_price)
        prev_price_float = _to_float(prev_price)
        current_original_float = _to_float(current_original)

        price_change = None
        price_change_percent = None
        if current_price_float is not None and prev_price_float is not None:
            price_change = current_price_float - prev_price_float
            if prev_price_float > 0:
                price_change_percent = round(price_change / prev_price_float * 100, 2)

        discount_amount = None
        if current_original_float is not None and current_price_float is not None:
            discount_amount = current_original_float - current_price_float

        if prev is None:
            crawl_type = "initial_snapshot"
        elif price_changed or original_changed or discount_changed:
            crawl_type = "price_change"
        elif sales_changed:
            crawl_type = "sales_change"
        else:
            crawl_type = "metric_change"

        is_flash_sale = bool(
            (current_discount is not None and current_discount >= 30)
            or (discount_amount is not None and discount_amount >= 100000)
        )

        rows.append(
            (
                product_id,
                crawl_run_id,
                current_price,
                current_original,
                current_discount,
                discount_amount,
                price_change,
                price_change_percent,
                prev_price,
                prev_original,
                prev_discount,
                current_sales,
                sales_change,
                current_rating,
                current_reviews,
                is_flash_sale,
                crawl_type,
                product.get("crawled_at") or now,
                now,
            )
        )

    if not rows:
        return 0

    execute_values(
        cursor,
        """
        INSERT INTO crawl_history (
            product_id, crawl_run_id, price, original_price, discount_percent, discount_amount,
            price_change, price_change_percent,
            previous_price, previous_original_price, previous_discount_percent,
            sales_count, sales_change, rating_average, review_count,
            is_flash_sale, crawl_type, crawled_at, updated_at
        )
        VALUES %s
        ON CONFLICT (product_id, crawl_run_id) WHERE crawl_run_id IS NOT NULL DO UPDATE SET
            price = EXCLUDED.price,
            original_price = EXCLUDED.original_price,
            discount_percent = EXCLUDED.discount_percent,
            discount_amount = EXCLUDED.discount_amount,
            price_change = EXCLUDED.price_change,
            price_change_percent = EXCLUDED.price_change_percent,
            previous_price = EXCLUDED.previous_price,
            previous_original_price = EXCLUDED.previous_original_price,
            previous_discount_percent = EXCLUDED.previous_discount_percent,
            sales_count = EXCLUDED.sales_count,
            sales_change = EXCLUDED.sales_change,
            rating_average = EXCLUDED.rating_average,
            review_count = EXCLUDED.review_count,
            is_flash_sale = EXCLUDED.is_flash_sale,
            crawl_type = EXCLUDED.crawl_type,
            crawled_at = EXCLUDED.crawled_at,
            updated_at = EXCLUDED.updated_at
        """,
        rows,
        page_size=500,
    )
    return len(rows)


def load_layered_products(
    cursor: Any, products: list[dict[str, Any]], crawl_run_id: int | None
) -> int:
    """Load normalized products into sellers/products and price/metric snapshots."""
    if not products:
        return 0

    now = datetime.now()
    seller_rows = []
    seen_sellers = set()
    for product in products:
        seller_id = product.get("seller_id")
        seller_name = product.get("seller_name")
        if not seller_id and not seller_name:
            continue
        seller_key = str(seller_id or seller_name)
        if seller_key in seen_sellers:
            continue
        seen_sellers.add(seller_key)
        seller_rows.append(
            (
                str(seller_id) if seller_id else None,
                seller_name,
                product.get("seller_is_official"),
                Json(
                    {
                        "seller_id": seller_id,
                        "seller_name": seller_name,
                        "seller_is_official": product.get("seller_is_official"),
                    }
                ),
            )
        )

    if seller_rows:
        execute_values(
            cursor,
            """
            INSERT INTO sellers (tiki_seller_id, name, is_official, raw_json)
            VALUES %s
            ON CONFLICT (tiki_seller_id) DO UPDATE SET
                name = COALESCE(EXCLUDED.name, sellers.name),
                is_official = COALESCE(EXCLUDED.is_official, sellers.is_official),
                raw_json = COALESCE(EXCLUDED.raw_json, sellers.raw_json)
            """,
            seller_rows,
            page_size=500,
        )

    product_rows = []
    for product in products:
        product_id = product.get("product_id")
        if not product_id:
            continue
        product_rows.append(
            (
                str(product_id),
                str(product_id),
                product.get("name"),
                product.get("short_name"),
                product.get("brand"),
                product.get("url"),
                product.get("image_url"),
                Json(product["images"]) if product.get("images") is not None else None,
                product.get("description"),
                product.get("description_text"),
                product.get("category_id"),
                product.get("category_url"),
                product.get("seller_id"),
                product.get("seller_name"),
                product.get("seller_is_official"),
                product.get("price"),
                product.get("original_price"),
                product.get("discount_percent"),
                product.get("rating_average"),
                product.get("review_count"),
                product.get("sales_count"),
                product.get("stock_available"),
                Json(product["shipping"]) if product.get("shipping") else None,
                product.get("detail_status"),
                Json(product["missing_fields"]) if product.get("missing_fields") is not None else None,
                product.get("data_quality_score"),
                product.get("crawl_quality", "basic"),
                product.get("needs_enrichment", False),
                product.get("crawled_at") or now,
                now,
            )
        )

    execute_values(
        cursor,
        """
        INSERT INTO products (
            tiki_product_id, product_id, name, short_name, brand, url, image_url, images,
            description, description_text,
            category_id, category_url, seller_id, seller_name, seller_is_official,
            price, original_price, discount_percent, rating_average, review_count, sales_count,
            stock_available, shipping, detail_status, missing_fields, data_quality_score,
            crawl_quality, needs_enrichment, crawled_at, updated_at
        )
        VALUES %s
        ON CONFLICT (product_id) DO UPDATE SET
            tiki_product_id = EXCLUDED.tiki_product_id,
            name = COALESCE(NULLIF(EXCLUDED.name, ''), products.name),
            short_name = COALESCE(NULLIF(EXCLUDED.short_name, ''), products.short_name),
            brand = COALESCE(NULLIF(EXCLUDED.brand, ''), products.brand),
            url = COALESCE(NULLIF(EXCLUDED.url, ''), products.url),
            image_url = COALESCE(NULLIF(EXCLUDED.image_url, ''), products.image_url),
            images = COALESCE(EXCLUDED.images, products.images),
            description = COALESCE(NULLIF(EXCLUDED.description, ''), products.description),
            description_text = COALESCE(NULLIF(EXCLUDED.description_text, ''), products.description_text),
            category_id = COALESCE(EXCLUDED.category_id, products.category_id),
            category_url = COALESCE(EXCLUDED.category_url, products.category_url),
            seller_id = COALESCE(EXCLUDED.seller_id, products.seller_id),
            seller_name = COALESCE(NULLIF(EXCLUDED.seller_name, ''), products.seller_name),
            seller_is_official = COALESCE(EXCLUDED.seller_is_official, products.seller_is_official),
            price = COALESCE(EXCLUDED.price, products.price),
            original_price = COALESCE(EXCLUDED.original_price, products.original_price),
            discount_percent = COALESCE(EXCLUDED.discount_percent, products.discount_percent),
            rating_average = COALESCE(EXCLUDED.rating_average, products.rating_average),
            review_count = COALESCE(EXCLUDED.review_count, products.review_count),
            sales_count = COALESCE(EXCLUDED.sales_count, products.sales_count),
            stock_available = COALESCE(EXCLUDED.stock_available, products.stock_available),
            shipping = COALESCE(EXCLUDED.shipping, products.shipping),
            detail_status = COALESCE(EXCLUDED.detail_status, products.detail_status),
            missing_fields = COALESCE(EXCLUDED.missing_fields, products.missing_fields),
            data_quality_score = COALESCE(EXCLUDED.data_quality_score, products.data_quality_score),
            crawl_quality = EXCLUDED.crawl_quality,
            needs_enrichment = EXCLUDED.needs_enrichment,
            updated_at = EXCLUDED.updated_at
        """,
        product_rows,
        page_size=500,
    )

    cursor.execute(
        """
        UPDATE products p
        SET seller_pk = s.id
        FROM sellers s
        WHERE p.seller_id IS NOT NULL AND s.tiki_seller_id = p.seller_id
          AND p.product_id = ANY(%s)
        """,
        ([row[1] for row in product_rows],),
    )
    cursor.execute(
        """
        UPDATE products p
        SET category_pk = c.id
        FROM categories c
        WHERE p.category_id IS NOT NULL AND c.category_id = p.category_id
          AND p.product_id = ANY(%s)
        """,
        ([row[1] for row in product_rows],),
    )

    cursor.execute(
        "SELECT product_id, id FROM products WHERE product_id = ANY(%s)",
        ([row[1] for row in product_rows],),
    )
    product_pk_by_id = {row[0]: row[1] for row in cursor.fetchall()}

    price_rows = []
    metric_rows = []
    for product in products:
        product_pk = product_pk_by_id.get(str(product.get("product_id")))
        if not product_pk:
            continue
        if any(
            product.get(key) is not None for key in ("price", "original_price", "discount_percent")
        ):
            price_rows.append(
                (
                    product_pk,
                    crawl_run_id,
                    product.get("price"),
                    product.get("original_price"),
                    product.get("discount_percent"),
                    product.get("crawled_at") or now,
                )
            )
        if any(
            product.get(key) is not None
            for key in ("rating_average", "review_count", "sales_count")
        ):
            metric_rows.append(
                (
                    product_pk,
                    crawl_run_id,
                    product.get("rating_average"),
                    product.get("review_count"),
                    product.get("sales_count"),
                    product.get("crawled_at") or now,
                )
            )

    if price_rows:
        execute_values(
            cursor,
            """
            INSERT INTO product_prices (
                product_id, crawl_run_id, price, original_price, discount_rate, recorded_at
            )
            VALUES %s
            ON CONFLICT (product_id, crawl_run_id) DO UPDATE SET
                price = EXCLUDED.price,
                original_price = EXCLUDED.original_price,
                discount_rate = EXCLUDED.discount_rate,
                recorded_at = EXCLUDED.recorded_at
            """,
            price_rows,
            page_size=500,
        )

    if metric_rows:
        execute_values(
            cursor,
            """
            INSERT INTO product_metrics (
                product_id, crawl_run_id, rating_average, review_count, quantity_sold, recorded_at
            )
            VALUES %s
            ON CONFLICT (product_id, crawl_run_id) DO UPDATE SET
                rating_average = EXCLUDED.rating_average,
                review_count = EXCLUDED.review_count,
                quantity_sold = EXCLUDED.quantity_sold,
                recorded_at = EXCLUDED.recorded_at
            """,
            metric_rows,
            page_size=500,
        )

    _write_crawl_history_events(cursor, products, product_pk_by_id, crawl_run_id, now)
    return len(product_rows)


__all__ = [
    "finish_crawl_run",
    "load_layered_products",
    "save_raw_products",
    "start_crawl_run",
    "upsert_crawl_task",
    "upsert_crawl_tasks",
]
