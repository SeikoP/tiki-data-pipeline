from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from common.settings import get_pipeline_settings  # noqa: E402


def _connect():
    import psycopg2
    from psycopg2.extras import RealDictCursor

    db = get_pipeline_settings().database
    return psycopg2.connect(
        host=db.host,
        port=db.port,
        dbname=db.database,
        user=db.user,
        password=db.password,
        cursor_factory=RealDictCursor,
    )


def _fetch_one(conn, sql: str, params: tuple[Any, ...] = ()) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
    return dict(row) if row else {}


def _fetch_all(conn, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def _resolve_crawl_run_id(conn, crawl_run_id: str) -> int:
    if crawl_run_id != "latest":
        return int(crawl_run_id)

    row = _fetch_one(
        conn,
        """
        SELECT id
        FROM crawl_runs
        ORDER BY started_at DESC NULLS LAST, id DESC
        LIMIT 1
        """,
    )
    if not row:
        raise RuntimeError("No crawl_runs rows found")
    return int(row["id"])


def collect_report(conn, crawl_run_id: int) -> dict[str, Any]:
    run = _fetch_one(
        conn,
        """
        SELECT id, source, airflow_dag_id, airflow_run_id, started_at, finished_at, status,
               total_raw_items, total_transformed_items, total_loaded_items, error_message
        FROM crawl_runs
        WHERE id = %s
        """,
        (crawl_run_id,),
    )
    if not run:
        raise RuntimeError(f"crawl_run_id={crawl_run_id} not found")

    raw_by_stage = _fetch_all(
        conn,
        """
        SELECT source_stage,
               COUNT(*) AS rows,
               COUNT(DISTINCT tiki_product_id) AS distinct_products,
               COUNT(*) - COUNT(DISTINCT tiki_product_id) AS duplicate_rows,
               COUNT(*) FILTER (
                   WHERE tiki_product_id IS NULL OR tiki_product_id = ''
               ) AS missing_product_id,
               COUNT(*) FILTER (
                   WHERE category_id IS NULL OR category_id = ''
               ) AS missing_category_id,
               MIN(crawled_at) AS min_crawled_at,
               MAX(crawled_at) AS max_crawled_at
        FROM raw_products
        WHERE crawl_run_id = %s
        GROUP BY source_stage
        ORDER BY source_stage
        """,
        (crawl_run_id,),
    )

    coverage = _fetch_one(
        conn,
        """
        WITH list AS (
            SELECT DISTINCT tiki_product_id
            FROM raw_products
            WHERE crawl_run_id = %s AND source_stage = 'list'
        ),
        detail AS (
            SELECT DISTINCT tiki_product_id
            FROM raw_products
            WHERE crawl_run_id = %s AND source_stage = 'detail'
        ),
        loaded AS (
            SELECT DISTINCT p.tiki_product_id
            FROM product_prices pp
            JOIN products p ON p.id = pp.product_id
            WHERE pp.crawl_run_id = %s
        )
        SELECT (SELECT COUNT(*) FROM list) AS list_distinct,
               (SELECT COUNT(*) FROM detail) AS detail_distinct,
               (SELECT COUNT(*) FROM loaded) AS loaded_distinct,
               (SELECT COUNT(*) FROM detail d JOIN list l USING (tiki_product_id))
                   AS detail_in_list,
               (SELECT COUNT(*) FROM loaded l JOIN detail d USING (tiki_product_id))
                   AS loaded_in_detail
        """,
        (crawl_run_id, crawl_run_id, crawl_run_id),
    )

    loaded_quality = _fetch_one(
        conn,
        """
        WITH loaded AS (
            SELECT DISTINCT pp.product_id
            FROM product_prices pp
            WHERE pp.crawl_run_id = %s
        )
        SELECT COUNT(*) AS loaded_products,
               COUNT(*) FILTER (
                   WHERE p.tiki_product_id IS NULL OR p.tiki_product_id = ''
               ) AS missing_tiki_id,
               COUNT(*) FILTER (WHERE p.name IS NULL OR BTRIM(p.name) = '') AS missing_name,
               COUNT(*) FILTER (WHERE p.url IS NULL OR BTRIM(p.url) = '') AS missing_url,
               COUNT(*) FILTER (
                   WHERE p.image_url IS NULL OR BTRIM(p.image_url) = ''
               ) AS missing_image_url,
               COUNT(*) FILTER (
                   WHERE p.category_id IS NULL OR BTRIM(p.category_id) = ''
               ) AS missing_category_id,
               COUNT(*) FILTER (WHERE p.category_pk IS NULL) AS missing_category_fk,
               COUNT(*) FILTER (
                   WHERE p.seller_name IS NULL OR BTRIM(p.seller_name) = ''
               ) AS missing_seller_name,
               COUNT(*) FILTER (
                   WHERE p.description_text IS NULL OR BTRIM(p.description_text) = ''
               ) AS missing_description_text,
               COUNT(*) FILTER (WHERE p.needs_enrichment IS TRUE) AS needs_enrichment,
               AVG(p.data_quality_score) AS avg_quality_score
        FROM loaded l
        JOIN products p ON p.id = l.product_id
        """,
        (crawl_run_id,),
    )

    price = _fetch_one(
        conn,
        """
        SELECT COUNT(*) AS price_rows,
               COUNT(DISTINCT product_id) AS priced_products,
               COUNT(*) FILTER (WHERE price IS NULL) AS null_price,
               COUNT(*) FILTER (WHERE price <= 0) AS non_positive_price,
               COUNT(*) FILTER (
                   WHERE original_price IS NOT NULL AND original_price < price
               ) AS original_less_than_price,
               COUNT(*) FILTER (
                   WHERE discount_rate IS NOT NULL
                     AND (discount_rate < 0 OR discount_rate > 100)
               ) AS invalid_discount_rate
        FROM product_prices
        WHERE crawl_run_id = %s
        """,
        (crawl_run_id,),
    )

    metrics = _fetch_one(
        conn,
        """
        SELECT COUNT(*) AS metric_rows,
               COUNT(DISTINCT product_id) AS metric_products,
               COUNT(*) FILTER (WHERE rating_average IS NULL) AS null_rating,
               COUNT(*) FILTER (
                   WHERE rating_average < 0 OR rating_average > 5
               ) AS invalid_rating,
               COUNT(*) FILTER (WHERE review_count < 0) AS negative_review_count,
               COUNT(*) FILTER (WHERE quantity_sold < 0) AS negative_quantity_sold
        FROM product_metrics
        WHERE crawl_run_id = %s
        """,
        (crawl_run_id,),
    )

    integrity = _fetch_one(
        conn,
        """
        WITH loaded AS (
            SELECT DISTINCT pp.product_id
            FROM product_prices pp
            WHERE pp.crawl_run_id = %s
        )
        SELECT COUNT(*) AS loaded_products,
               COUNT(*) FILTER (WHERE s.id IS NULL) AS seller_fk_missing,
               COUNT(*) FILTER (WHERE c.id IS NULL) AS category_fk_missing,
               COUNT(*) FILTER (
                   WHERE p.category_pk IS NOT NULL
                     AND p.category_id IS DISTINCT FROM c.category_id
                     AND p.category_id IS DISTINCT FROM c.tiki_category_id
               ) AS category_key_mismatch
        FROM loaded l
        JOIN products p ON p.id = l.product_id
        LEFT JOIN sellers s ON s.id = p.seller_pk
        LEFT JOIN categories c ON c.id = p.category_pk
        """,
        (crawl_run_id,),
    )

    snapshots = _fetch_one(
        conn,
        """
        WITH loaded AS (
            SELECT DISTINCT pp.product_id
            FROM product_prices pp
            WHERE pp.crawl_run_id = %s
        )
        SELECT COUNT(*) FILTER (
                   WHERE p.price IS DISTINCT FROM pp.price
               ) AS product_price_snapshot_mismatch,
               COUNT(*) FILTER (
                   WHERE p.rating_average IS DISTINCT FROM pm.rating_average
               ) AS product_rating_snapshot_mismatch,
               COUNT(*) FILTER (
                   WHERE p.review_count IS DISTINCT FROM pm.review_count
               ) AS product_review_snapshot_mismatch,
               COUNT(*) FILTER (
                   WHERE p.sales_count IS DISTINCT FROM pm.quantity_sold
               ) AS product_sales_snapshot_mismatch
        FROM loaded l
        JOIN products p ON p.id = l.product_id
        LEFT JOIN product_prices pp ON pp.product_id = p.id AND pp.crawl_run_id = %s
        LEFT JOIN product_metrics pm ON pm.product_id = p.id AND pm.crawl_run_id = %s
        """,
        (crawl_run_id, crawl_run_id, crawl_run_id),
    )

    tasks = _fetch_all(
        conn,
        """
        SELECT task_type, status, COUNT(*) AS tasks,
               COUNT(*) FILTER (
                   WHERE error_message IS NOT NULL AND BTRIM(error_message) <> ''
               ) AS tasks_with_error
        FROM crawl_tasks
        WHERE crawl_run_id = %s
        GROUP BY task_type, status
        ORDER BY task_type, status
        """,
        (crawl_run_id,),
    )

    critical = {
        "raw_duplicate_rows": sum(int(row["duplicate_rows"]) for row in raw_by_stage),
        "missing_loaded_required_fields": sum(
            int(loaded_quality[key])
            for key in (
                "missing_tiki_id",
                "missing_name",
                "missing_url",
                "missing_category_id",
                "missing_category_fk",
                "missing_seller_name",
            )
        ),
        "invalid_prices": sum(
            int(price[key])
            for key in (
                "null_price",
                "non_positive_price",
                "original_less_than_price",
                "invalid_discount_rate",
            )
        ),
        "invalid_metrics": sum(
            int(metrics[key])
            for key in ("invalid_rating", "negative_review_count", "negative_quantity_sold")
        ),
        "fk_integrity_errors": sum(
            int(integrity[key])
            for key in ("seller_fk_missing", "category_fk_missing", "category_key_mismatch")
        ),
        "snapshot_mismatches": sum(int(value or 0) for value in snapshots.values()),
    }
    warnings = {
        "raw_missing_category_id": sum(int(row["missing_category_id"]) for row in raw_by_stage),
        "loaded_needs_enrichment": int(loaded_quality["needs_enrichment"] or 0),
        "missing_task_audit_rows": 1 if not tasks else 0,
    }

    return {
        "crawl_run": run,
        "raw_by_stage": raw_by_stage,
        "coverage": coverage,
        "loaded_quality": loaded_quality,
        "price": price,
        "metrics": metrics,
        "integrity": integrity,
        "snapshots": snapshots,
        "tasks": tasks,
        "critical": critical,
        "warnings": warnings,
        "status": "failed" if any(value > 0 for value in critical.values()) else "passed",
    }


def _json_default(value: Any) -> str:
    return str(value)


def print_table(report: dict[str, Any]) -> None:
    run = report["crawl_run"]
    coverage = report["coverage"]
    loaded = report["loaded_quality"]

    print("Latest crawl quality report")
    print("=" * 32)
    print(f"crawl_run_id: {run['id']}")
    print(f"dag/run:       {run['airflow_dag_id']} / {run['airflow_run_id']}")
    print(f"status:        {run['status']} ({report['status']})")
    print(f"started_at:    {run['started_at']}")
    print(f"finished_at:   {run['finished_at']}")
    print()

    print("Coverage")
    print(f"  raw list distinct:   {coverage['list_distinct']}")
    print(f"  raw detail distinct: {coverage['detail_distinct']}")
    print(f"  loaded distinct:     {coverage['loaded_distinct']}")
    print(f"  loaded in detail:    {coverage['loaded_in_detail']}")
    print()

    print("Loaded quality")
    print(f"  loaded products:        {loaded['loaded_products']}")
    print(f"  needs enrichment:       {loaded['needs_enrichment']}")
    print(f"  avg quality score:      {loaded['avg_quality_score']}")
    print()

    print("Critical checks")
    for name, value in report["critical"].items():
        marker = "FAIL" if value else "OK"
        print(f"  {marker:4} {name}: {value}")
    print()

    print("Warnings")
    for name, value in report["warnings"].items():
        marker = "WARN" if value else "OK"
        print(f"  {marker:4} {name}: {value}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Check latest Tiki crawl DB data quality.")
    parser.add_argument("--crawl-run-id", default="latest", help="crawl_runs.id or 'latest'")
    parser.add_argument("--format", choices=("table", "json"), default="table")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    with _connect() as conn:
        crawl_run_id = _resolve_crawl_run_id(conn, args.crawl_run_id)
        report = collect_report(conn, crawl_run_id)

    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2, default=_json_default))
    else:
        print_table(report)

    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
