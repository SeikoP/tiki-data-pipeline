"""
Incremental Crawler Helper

Provides smart scheduling logic for incremental crawling:
- Priority queue based on last_crawled_at
- Skip recently crawled products
- Prioritize products with frequent price changes
"""

from typing import Any


def get_products_to_crawl(
    storage, max_products: int = 1000, min_hours_since_crawl: int = 24, prioritize_hot: bool = True
) -> list[dict[str, Any]]:
    """
    Get list of products that need to be crawled based on smart scheduling

    Args:
        storage: PostgresStorage instance
        max_products: Maximum number of products to return
        min_hours_since_crawl: Minimum hours since last crawl to include product
        prioritize_hot: If True, prioritize products with frequent price changes

    Returns:
        List of product dictionaries to crawl
    """
    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            if prioritize_hot:
                # Priority 1: Products with frequent price changes (hot products)
                # Priority 2: Products not crawled recently
                # Priority 3: Never crawled products
                query = """
                    WITH product_stats AS (
                        SELECT
                            p.product_id,
                            p.name,
                            p.url,
                            p.category_id,
                            p.last_crawled_at,
                            p.last_crawl_attempt_at,
                            COUNT(h.id) as price_change_count,
                            MAX(h.crawled_at) as last_price_change
                        FROM products p
                        LEFT JOIN crawl_history h ON p.product_id = h.product_id
                            AND h.crawl_type = 'price_change'
                            AND h.crawled_at > NOW() - INTERVAL '30 days'
                        GROUP BY p.product_id, p.name, p.url, p.category_id,
                                 p.last_crawled_at, p.last_crawl_attempt_at
                    )
                    SELECT
                        product_id,
                        name,
                        url,
                        category_id,
                        last_crawled_at,
                        price_change_count,
                        CASE
                            WHEN last_crawled_at IS NULL THEN 1  -- Never crawled
                            WHEN price_change_count > 5 THEN 2   -- Hot product
                            WHEN last_crawled_at < NOW() - INTERVAL '%s hours' THEN 3  -- Stale
                            ELSE 4  -- Recently crawled
                        END as priority
                    FROM product_stats
                    WHERE last_crawled_at IS NULL
                       OR last_crawled_at < NOW() - INTERVAL '%s hours'
                    ORDER BY priority ASC, price_change_count DESC, last_crawled_at ASC NULLS FIRST
                    LIMIT %s
                """
                cur.execute(query, (min_hours_since_crawl, min_hours_since_crawl, max_products))
            else:
                # Simple: Just get products not crawled recently
                query = """
                    SELECT
                        product_id,
                        name,
                        url,
                        category_id,
                        last_crawled_at
                    FROM products
                    WHERE last_crawled_at IS NULL
                       OR last_crawled_at < NOW() - INTERVAL '%s hours'
                    ORDER BY last_crawled_at ASC NULLS FIRST
                    LIMIT %s
                """
                cur.execute(query, (min_hours_since_crawl, max_products))

            rows = cur.fetchall()

            # Convert to list of dicts
            products = []
            for row in rows:
                products.append(
                    {
                        "product_id": row[0],
                        "name": row[1],
                        "url": row[2],
                        "category_id": row[3],
                        "last_crawled_at": row[4],
                    }
                )

            return products


def get_crawl_statistics(storage) -> dict[str, Any]:
    """
    Get crawl statistics for monitoring

    Returns:
        Dictionary with crawl stats
    """
    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            # Overall stats
            cur.execute("""
                SELECT
                    COUNT(*) as total_products,
                    COUNT(last_crawled_at) as crawled_products,
                    COUNT(CASE WHEN last_crawled_at > NOW() - INTERVAL '24 hours' THEN 1 END) as crawled_24h,
                    COUNT(CASE WHEN last_crawled_at > NOW() - INTERVAL '7 days' THEN 1 END) as crawled_7d,
                    COUNT(CASE WHEN last_crawled_at IS NULL THEN 1 END) as never_crawled
                FROM products
            """)

            stats = cur.fetchone()

            # Price change frequency
            cur.execute("""
                SELECT
                    COUNT(DISTINCT product_id) as products_with_changes,
                    COUNT(*) as total_changes,
                    AVG(price_change) as avg_price_change
                FROM crawl_history
                WHERE crawl_type = 'price_change'
                  AND crawled_at > NOW() - INTERVAL '30 days'
            """)

            price_stats = cur.fetchone()

            return {
                "total_products": stats[0],
                "crawled_products": stats[1],
                "crawled_24h": stats[2],
                "crawled_7d": stats[3],
                "never_crawled": stats[4],
                "products_with_price_changes_30d": price_stats[0] if price_stats else 0,
                "total_price_changes_30d": price_stats[1] if price_stats else 0,
                "avg_price_change_30d": (
                    float(price_stats[2]) if price_stats and price_stats[2] else 0
                ),
            }


if __name__ == "__main__":
    # Example usage
    import os
    import sys
    from pathlib import Path

    src_path = Path(__file__).parent.parent / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))

    from pipelines.crawl.storage.postgres_storage import PostgresStorage

    if "POSTGRES_HOST" not in os.environ:
        os.environ["POSTGRES_HOST"] = "localhost"

    storage = PostgresStorage()

    print("📊 Crawl Statistics:")
    stats = get_crawl_statistics(storage)
    for key, value in stats.items():
        print(f"   {key}: {value}")

    print("\n🎯 Products to crawl (top 10):")
    products = get_products_to_crawl(storage, max_products=10, min_hours_since_crawl=24)
    for i, p in enumerate(products, 1):
        last_crawl = (
            p["last_crawled_at"].strftime("%Y-%m-%d %H:%M") if p["last_crawled_at"] else "Never"
        )
        print(f"   {i}. {p['name'][:50]} (last: {last_crawl})")
