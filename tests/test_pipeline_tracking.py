from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))

import common.pipeline_tracking as tracking  # noqa: E402
from common.pipeline_tracking import _raw_category_id  # noqa: E402


def test_raw_category_id_preserves_existing_category_id() -> None:
    product = {
        "product_id": "123",
        "category_id": "c23282",
        "category_url": "https://tiki.vn/binh-dung-nuoc/c23282",
    }

    assert _raw_category_id(product) == "c23282"


def test_raw_category_id_normalizes_numeric_category_id() -> None:
    product = {
        "product_id": "123",
        "category_id": "23282",
        "category_url": "https://tiki.vn/binh-dung-nuoc/c23282",
    }

    assert _raw_category_id(product) == "c23282"


def test_raw_category_id_derives_from_category_url() -> None:
    product = {
        "product_id": "123",
        "category_url": "https://tiki.vn/binh-dung-nuoc/c23282?src=search",
    }

    assert _raw_category_id(product) == "c23282"


def test_raw_category_id_stays_empty_without_category_url() -> None:
    assert _raw_category_id({"product_id": "123"}) is None


def test_crawl_history_event_logs_initial_snapshot(monkeypatch) -> None:
    captured = {}

    class Cursor:
        def execute(self, *_args, **_kwargs):
            return None

        def fetchall(self):
            return []

    def fake_execute_values(_cursor, _sql, rows, page_size=500):
        captured["rows"] = rows
        captured["page_size"] = page_size

    monkeypatch.setattr(tracking, "execute_values", fake_execute_values)

    count = tracking._write_crawl_history_events(
        Cursor(),
        [
            {
                "product_id": "123",
                "price": 100000,
                "original_price": 120000,
                "discount_percent": 17,
                "rating_average": None,
                "review_count": 0,
                "sales_count": None,
            }
        ],
        {"123": 1},
        crawl_run_id=10,
        now=tracking.datetime(2026, 1, 1),
    )

    assert count == 1
    row = captured["rows"][0]
    assert row[0] == "123"
    assert row[1] == 10
    assert row[16] == "initial_snapshot"
    assert row[17] == tracking.datetime(2026, 1, 1)
