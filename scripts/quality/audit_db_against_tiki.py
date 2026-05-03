from __future__ import annotations

import argparse
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SRC_PATH = PROJECT_ROOT / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.insert(0, str(SRC_PATH))

from common.settings import get_float_setting, get_int_setting, get_pipeline_settings  # isort: skip  # noqa: E402


CORE_COMPARE_FIELDS = (
    "name",
    "brand",
    "seller_name",
    "price",
    "original_price",
    "rating_average",
    "review_count",
    "sales_count",
    "stock_available",
    "stock_status",
)


@dataclass(frozen=True)
class AuditConfig:
    crawl_run_id: int
    category_ids: tuple[str, ...]
    output_dir: Path
    http_timeout: int
    request_delay: float
    max_products: int


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


def resolve_crawl_run_id(conn, crawl_run_id: str) -> int:
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


def _to_int(value: Any) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, dict):
        value = value.get("value")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value)).quantize(Decimal("0.01"))
    except Exception:
        return None


def _normalize_text(value: Any) -> str | None:
    if value in (None, ""):
        return None
    text = " ".join(str(value).split())
    return text or None


def normalize_stock_status(value: Any) -> str:
    if value in (None, ""):
        return "unknown"
    status = str(value).strip().lower()
    aliases = {
        "in_stock": "available",
        "instock": "available",
        "available": "available",
        "out_of_stock": "out_of_stock",
        "outofstock": "out_of_stock",
        "unavailable": "out_of_stock",
        "sold_out": "out_of_stock",
    }
    return aliases.get(status, status or "unknown")


def _stock_available_from_live(live: dict[str, Any]) -> bool | None:
    status = normalize_stock_status(live.get("inventory_status"))
    if status == "available":
        return True
    if status == "out_of_stock":
        return False

    stock_item = live.get("stock_item")
    if isinstance(stock_item, dict) and "qty" in stock_item:
        qty = _to_int(stock_item.get("qty"))
        return None if qty is None else qty > 0
    return None


def normalize_live_product(live: dict[str, Any]) -> dict[str, Any]:
    seller = live.get("current_seller")
    if not isinstance(seller, dict):
        seller = live.get("seller") if isinstance(live.get("seller"), dict) else {}
    brand = live.get("brand")
    brand_name = brand.get("name") if isinstance(brand, dict) else brand
    sales_count = live.get("quantity_sold")
    if isinstance(sales_count, dict):
        sales_count = sales_count.get("value")

    stock_available = _stock_available_from_live(live)
    stock_status = normalize_stock_status(live.get("inventory_status"))
    if stock_status == "unknown" and stock_available is not None:
        stock_status = "available" if stock_available else "out_of_stock"

    return {
        "name": _normalize_text(live.get("name")),
        "brand": _normalize_text(live.get("brand_name") or brand_name),
        "seller_name": _normalize_text(seller.get("name")),
        "price": _to_decimal(live.get("price")),
        "original_price": _to_decimal(live.get("original_price")),
        "rating_average": _to_decimal(live.get("rating_average")),
        "review_count": _to_int(live.get("review_count")),
        "sales_count": _to_int(sales_count),
        "stock_available": stock_available,
        "stock_status": stock_status,
    }


def normalize_db_product(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": _normalize_text(row.get("name")),
        "brand": _normalize_text(row.get("brand")),
        "seller_name": _normalize_text(row.get("seller_name")),
        "price": _to_decimal(row.get("price")),
        "original_price": _to_decimal(row.get("original_price")),
        "rating_average": _to_decimal(row.get("rating_average")),
        "review_count": _to_int(row.get("review_count")),
        "sales_count": _to_int(row.get("sales_count")),
        "stock_available": row.get("stock_available"),
        "stock_status": normalize_stock_status(row.get("stock_status")),
    }


def compare_products(db_product: dict[str, Any], live_product: dict[str, Any]) -> list[dict[str, Any]]:
    mismatches = []
    for field in CORE_COMPARE_FIELDS:
        db_value = db_product.get(field)
        live_value = live_product.get(field)
        if live_value is None:
            mismatches.append(
                {
                    "field": field,
                    "kind": "not_checkable",
                    "db": _jsonable(db_value),
                    "live": None,
                }
            )
        elif db_value != live_value:
            mismatches.append(
                {
                    "field": field,
                    "kind": "mismatch",
                    "db": _jsonable(db_value),
                    "live": _jsonable(live_value),
                }
            )
    return mismatches


def fetch_live_product(product_id: str, *, timeout: int) -> dict[str, Any]:
    response = requests.get(
        f"https://tiki.vn/api/v2/products/{product_id}",
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def fetch_products(conn, config: AuditConfig) -> list[dict[str, Any]]:
    params: list[Any] = [config.crawl_run_id]
    category_clause = ""
    if config.category_ids:
        category_clause = "AND p.category_id = ANY(%s)"
        params.append(list(config.category_ids))

    limit_clause = ""
    if config.max_products > 0:
        limit_clause = "LIMIT %s"
        params.append(config.max_products)

    return _fetch_all(
        conn,
        f"""
        SELECT p.id, p.product_id, p.name, p.brand, p.seller_name,
               p.category_id, c.name AS category_name,
               p.price, p.original_price, p.rating_average, p.review_count,
               p.sales_count, p.stock_available, p.stock_status, p.stock_source
        FROM product_prices pp
        JOIN products p ON p.id = pp.product_id
        LEFT JOIN categories c ON c.category_id = p.category_id
        WHERE pp.crawl_run_id = %s
          {category_clause}
        ORDER BY p.category_id, p.product_id
        {limit_clause}
        """,
        tuple(params),
    )


def collect_db_summary(conn, crawl_run_id: int) -> dict[str, Any]:
    return {
        "counts": _fetch_one(
            conn,
            """
            SELECT
              (SELECT COUNT(*) FROM crawl_runs) AS crawl_runs,
              (SELECT COUNT(*) FROM crawl_tasks) AS crawl_tasks,
              (SELECT COUNT(*) FROM raw_products) AS raw_products,
              (SELECT COUNT(*) FROM sellers) AS sellers,
              (SELECT COUNT(*) FROM categories) AS categories,
              (SELECT COUNT(*) FROM products) AS products,
              (SELECT COUNT(*) FROM product_prices WHERE crawl_run_id = %s) AS product_prices,
              (SELECT COUNT(*) FROM product_metrics WHERE crawl_run_id = %s) AS product_metrics,
              (SELECT COUNT(*) FROM crawl_history WHERE crawl_run_id = %s) AS crawl_history
            """,
            (crawl_run_id, crawl_run_id, crawl_run_id),
        ),
        "field_quality": _fetch_one(
            conn,
            """
            SELECT COUNT(*) AS products,
                   COUNT(*) FILTER (WHERE name IS NULL OR btrim(name) = '') AS missing_name,
                   COUNT(*) FILTER (WHERE url IS NULL OR btrim(url) = '') AS missing_url,
                   COUNT(*) FILTER (WHERE price IS NULL) AS missing_price,
                   COUNT(*) FILTER (WHERE rating_average IS NULL) AS missing_rating,
                   COUNT(*) FILTER (WHERE review_count IS NULL) AS missing_review_count,
                   COUNT(*) FILTER (WHERE sales_count IS NULL) AS missing_sales_count,
                   COUNT(*) FILTER (WHERE stock_available IS NULL) AS unknown_stock,
                   COUNT(*) FILTER (WHERE seller_name IS NULL OR btrim(seller_name) = '') AS missing_seller,
                   COUNT(*) FILTER (WHERE brand IS NULL OR btrim(brand) = '') AS missing_brand,
                   COUNT(*) FILTER (WHERE category_id IS NULL OR btrim(category_id) = '') AS missing_category
            FROM products
            """,
        ),
        "integrity": _fetch_one(
            conn,
            """
            SELECT
              COUNT(*) FILTER (WHERE c.id IS NULL) AS products_missing_category_fk,
              COUNT(*) FILTER (WHERE s.id IS NULL AND p.seller_id IS NOT NULL)
                  AS products_missing_seller_fk,
              COUNT(*) FILTER (WHERE pp.id IS NULL) AS products_missing_price_snapshot,
              COUNT(*) FILTER (WHERE pm.id IS NULL) AS products_missing_metric_snapshot,
              COUNT(*) FILTER (WHERE ch.id IS NULL) AS products_missing_history
            FROM products p
            LEFT JOIN categories c ON c.category_id = p.category_id
            LEFT JOIN sellers s ON s.tiki_seller_id = p.seller_id
            LEFT JOIN product_prices pp ON pp.product_id = p.id AND pp.crawl_run_id = %s
            LEFT JOIN product_metrics pm ON pm.product_id = p.id AND pm.crawl_run_id = %s
            LEFT JOIN crawl_history ch ON ch.product_id = p.product_id AND ch.crawl_run_id = %s
            """,
            (crawl_run_id, crawl_run_id, crawl_run_id),
        ),
    }


def summarize_categories(product_results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for result in product_results:
        category_id = result["category_id"] or "unknown"
        item = grouped.setdefault(
            category_id,
            {
                "category_id": category_id,
                "category_name": result.get("category_name"),
                "products": 0,
                "matched": 0,
                "mismatched": 0,
                "live_errors": 0,
                "not_checkable_fields": 0,
                "mismatch_fields": {},
            },
        )
        item["products"] += 1
        if result["status"] == "matched":
            item["matched"] += 1
        elif result["status"] == "live_error":
            item["live_errors"] += 1
        else:
            item["mismatched"] += 1

        for mismatch in result.get("mismatches", []):
            if mismatch["kind"] == "not_checkable":
                item["not_checkable_fields"] += 1
            else:
                field = mismatch["field"]
                item["mismatch_fields"][field] = item["mismatch_fields"].get(field, 0) + 1

    return sorted(grouped.values(), key=lambda row: (row["category_id"]))


def run_audit(conn, config: AuditConfig) -> dict[str, Any]:
    products = fetch_products(conn, config)
    results = []
    critical_count = 0

    for index, row in enumerate(products):
        if index > 0 and config.request_delay > 0:
            time.sleep(config.request_delay)

        db_normalized = normalize_db_product(row)
        try:
            live = fetch_live_product(str(row["product_id"]), timeout=config.http_timeout)
            live_normalized = normalize_live_product(live)
            mismatches = compare_products(db_normalized, live_normalized)
            critical_mismatches = [item for item in mismatches if item["kind"] == "mismatch"]
            critical_count += len(critical_mismatches)
            status = "matched" if not critical_mismatches else "mismatched"
            live_error = None
        except Exception as exc:
            live_normalized = {}
            mismatches = []
            status = "live_error"
            live_error = str(exc)
            critical_count += 1

        results.append(
            {
                "product_id": row["product_id"],
                "category_id": row["category_id"],
                "category_name": row["category_name"],
                "status": status,
                "db": {key: _jsonable(value) for key, value in db_normalized.items()},
                "live": {key: _jsonable(value) for key, value in live_normalized.items()},
                "mismatches": mismatches,
                "live_error": live_error,
            }
        )

    db_summary = collect_db_summary(conn, config.crawl_run_id)
    integrity_errors = sum(int(value or 0) for value in db_summary["integrity"].values())
    required_errors = sum(
        int(db_summary["field_quality"].get(key) or 0)
        for key in (
            "missing_name",
            "missing_url",
            "missing_price",
            "missing_rating",
            "missing_review_count",
            "missing_seller",
            "missing_brand",
            "missing_category",
        )
    )
    total_critical = critical_count + integrity_errors + required_errors

    return {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "crawl_run_id": config.crawl_run_id,
            "category_ids": list(config.category_ids),
            "http_timeout": config.http_timeout,
            "request_delay": config.request_delay,
            "max_products": config.max_products,
            "total_products_checked": len(results),
        },
        "db_summary": db_summary,
        "category_summaries": summarize_categories(results),
        "product_results": results,
        "status": "failed" if total_critical else "passed",
        "critical_count": total_critical,
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def write_report(report: dict[str, Any], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    crawl_run_id = report["metadata"]["crawl_run_id"]
    path = output_dir / f"tiki_db_live_audit_run_{crawl_run_id}_{timestamp}.json"
    path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def print_table(report: dict[str, Any], output_path: Path | None = None) -> None:
    print("Tiki DB live audit")
    print("=" * 32)
    print(f"crawl_run_id:      {report['metadata']['crawl_run_id']}")
    print(f"products checked:  {report['metadata']['total_products_checked']}")
    print(f"status:            {report['status']}")
    print(f"critical_count:    {report['critical_count']}")
    if output_path:
        print(f"json_report:       {output_path}")
    print()

    print("DB integrity")
    for key, value in report["db_summary"]["integrity"].items():
        marker = "FAIL" if value else "OK"
        print(f"  {marker:4} {key}: {value}")
    print()

    print("By category")
    for row in report["category_summaries"]:
        fields = ", ".join(f"{key}={value}" for key, value in row["mismatch_fields"].items())
        fields = fields or "none"
        print(
            f"  {row['category_id']} | {row.get('category_name') or ''} | "
            f"products={row['products']} matched={row['matched']} "
            f"mismatched={row['mismatched']} live_errors={row['live_errors']} "
            f"fields={fields}"
        )


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit normalized DB data against live Tiki API.")
    parser.add_argument("--crawl-run-id", default="latest", help="crawl_runs.id or 'latest'")
    parser.add_argument(
        "--category-id",
        action="append",
        default=[],
        help="Category id to audit. Repeat to audit multiple categories.",
    )
    parser.add_argument("--format", choices=("table", "json"), default="table")
    parser.add_argument("--output-dir", default=str(PROJECT_ROOT / "data" / "reports"))
    parser.add_argument("--http-timeout", type=int, default=None)
    parser.add_argument("--request-delay", type=float, default=None)
    parser.add_argument("--max-products", type=int, default=None)
    return parser.parse_args(argv)


def build_config(conn, args: argparse.Namespace) -> AuditConfig:
    return AuditConfig(
        crawl_run_id=resolve_crawl_run_id(conn, args.crawl_run_id),
        category_ids=tuple(args.category_id or ()),
        output_dir=Path(args.output_dir),
        http_timeout=args.http_timeout
        if args.http_timeout is not None
        else get_int_setting("TIKI_AUDIT_HTTP_TIMEOUT", use_airflow=False),
        request_delay=args.request_delay
        if args.request_delay is not None
        else get_float_setting("TIKI_AUDIT_REQUEST_DELAY", use_airflow=False),
        max_products=args.max_products
        if args.max_products is not None
        else get_int_setting("TIKI_AUDIT_MAX_PRODUCTS", use_airflow=False),
    )


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    with _connect() as conn:
        config = build_config(conn, args)
        report = run_audit(conn, config)

    output_path = write_report(report, config.output_dir)
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print_table(report, output_path)
    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    raise SystemExit(main())
