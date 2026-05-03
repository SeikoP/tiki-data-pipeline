from __future__ import annotations

from decimal import Decimal

from scripts.quality.audit_db_against_tiki import (
    compare_products,
    normalize_db_product,
    normalize_live_product,
    summarize_categories,
    write_report,
)


def test_compare_products_passes_exact_scalar_match() -> None:
    db_product = normalize_db_product(
        {
            "name": "Product A",
            "brand": "Brand",
            "seller_name": "Seller",
            "price": Decimal("100000.00"),
            "original_price": Decimal("120000.00"),
            "rating_average": Decimal("4.50"),
            "review_count": 12,
            "sales_count": 20,
            "stock_available": True,
            "stock_status": "available",
        }
    )
    live_product = normalize_live_product(
        {
            "name": "Product A",
            "brand_name": "Brand",
            "current_seller": {"name": "Seller"},
            "price": 100000,
            "original_price": 120000,
            "rating_average": 4.5,
            "review_count": 12,
            "quantity_sold": {"value": 20},
            "inventory_status": "available",
        }
    )

    assert compare_products(db_product, live_product) == []


def test_compare_products_flags_strict_price_and_sales_mismatch() -> None:
    db_product = normalize_db_product(
        {
            "name": "Product A",
            "brand": "Brand",
            "seller_name": "Seller",
            "price": Decimal("100000.00"),
            "original_price": Decimal("120000.00"),
            "rating_average": Decimal("4.50"),
            "review_count": 12,
            "sales_count": None,
            "stock_available": True,
            "stock_status": "available",
        }
    )
    live_product = normalize_live_product(
        {
            "name": "Product A",
            "brand_name": "Brand",
            "current_seller": {"name": "Seller"},
            "price": 99000,
            "original_price": 120000,
            "rating_average": 4.5,
            "review_count": 12,
            "quantity_sold": {"value": 20},
            "inventory_status": "available",
        }
    )

    mismatches = compare_products(db_product, live_product)

    assert {item["field"] for item in mismatches if item["kind"] == "mismatch"} == {
        "price",
        "sales_count",
    }


def test_live_stock_out_of_stock_normalizes_to_false() -> None:
    live_product = normalize_live_product(
        {
            "name": "Product A",
            "price": 100000,
            "original_price": 100000,
            "rating_average": 0,
            "review_count": 0,
            "inventory_status": "out_of_stock",
        }
    )

    assert live_product["stock_available"] is False
    assert live_product["stock_status"] == "out_of_stock"


def test_summarize_categories_groups_mismatches() -> None:
    summary = summarize_categories(
        [
            {
                "category_id": "c1",
                "category_name": "Cat 1",
                "status": "matched",
                "mismatches": [],
            },
            {
                "category_id": "c1",
                "category_name": "Cat 1",
                "status": "mismatched",
                "mismatches": [{"field": "price", "kind": "mismatch"}],
            },
            {
                "category_id": "c2",
                "category_name": "Cat 2",
                "status": "live_error",
                "mismatches": [],
            },
        ]
    )

    by_category = {row["category_id"]: row for row in summary}
    assert by_category["c1"]["products"] == 2
    assert by_category["c1"]["matched"] == 1
    assert by_category["c1"]["mismatched"] == 1
    assert by_category["c1"]["mismatch_fields"] == {"price": 1}
    assert by_category["c2"]["live_errors"] == 1


def test_write_report_creates_json_file(tmp_path) -> None:
    report = {
        "metadata": {"crawl_run_id": 7},
        "status": "passed",
        "critical_count": 0,
        "category_summaries": [],
        "product_results": [],
    }

    path = write_report(report, tmp_path)

    assert path.exists()
    assert path.name.startswith("tiki_db_live_audit_run_7_")
    assert '"status": "passed"' in path.read_text(encoding="utf-8")
