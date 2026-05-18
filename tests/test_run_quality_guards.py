import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "airflow" / "dags"))

from tiki_crawl_products_v2.tasks import loader as dag_loader  # noqa: E402
from tiki_crawl_products_v2.tasks.detail import (  # noqa: E402
    _extend_detail_results_from_xcom,
)
from tiki_crawl_products_v2.tasks.detail_new import _get_detail_concurrency  # noqa: E402
from tiki_crawl_products_v2.tasks.loader import _resolve_product_load_mode  # noqa: E402

from common.batch_processor import BatchProcessor  # noqa: E402
from common.category_utils import normalize_category_tree  # noqa: E402
from common.detail_batch_planner import plan_detail_batches  # noqa: E402
from pipelines.load.load_categories_to_db import _select_categories_for_product_refs  # noqa: E402
from pipelines.load.loader import OptimizedDataLoader  # noqa: E402


def test_batch_processor_reports_batch_errors():
    processor = BatchProcessor(batch_size=2, show_progress=False, continue_on_error=True)

    def process(batch):
        if 3 in batch:
            raise RuntimeError("boom")

    stats = processor.process([1, 2, 3], process, total_count=3)

    assert stats["total_processed"] == 2
    assert stats["total_failed"] == 1
    assert stats["errors"] == ["Batch 2 failed: boom"]


def test_loader_raises_when_db_load_fails():
    loader = OptimizedDataLoader(enable_db=False, show_progress=False)
    loader.enable_db = True

    def fail_load(products, upsert):
        loader.stats["failed_count"] = len(products)
        loader.stats["errors"].append('column "review_count" does not exist')

    loader._load_to_database = fail_load

    with pytest.raises(RuntimeError, match="Database load failed"):
        loader.load_products(
            [{"product_id": "p1", "name": "Product 1"}],
            save_to_file=None,
            raise_on_db_error=True,
        )


def test_loader_file_only_success_does_not_mask_db_failure(tmp_path):
    loader = OptimizedDataLoader(enable_db=False, show_progress=False)
    loader.enable_db = True

    def fail_load(products, upsert):
        loader.stats["failed_count"] = len(products)
        loader.stats["errors"].append("db unavailable")

    loader._load_to_database = fail_load

    stats = loader.load_products(
        [{"product_id": "p1", "name": "Product 1"}],
        save_to_file=str(tmp_path / "products.json"),
        raise_on_db_error=False,
    )

    assert stats["db_loaded"] == 0
    assert stats["file_loaded"] == 1
    assert stats["success_count"] == 0
    assert stats["failed_count"] == 1


def test_normalize_category_tree_adds_id_leaf_and_path():
    categories = [
        {"name": "Root", "url": "https://tiki.vn/root/c1", "parent_url": None, "level": 0},
        {
            "name": "Child",
            "url": "https://tiki.vn/child/c2",
            "parent_url": "https://tiki.vn/root/c1",
            "level": 1,
        },
    ]

    normalized = normalize_category_tree(categories)
    root, child = normalized

    assert root["category_id"] == "c1"
    assert root["is_leaf"] is False
    assert child["category_id"] == "c2"
    assert child["is_leaf"] is True
    assert child["category_path"] == ["Root", "Child"]
    assert child["level_1"] == "Root"
    assert child["level_2"] == "Child"


@pytest.mark.parametrize(
    ("product_count", "active_batches", "expected_batches"),
    [
        (1000, 6, 6),
        (1500, 6, 6),
        (2000, 6, 6),
        (1000, 3, 3),
        (4, 6, 4),
    ],
)
def test_detail_batch_planner_fills_active_wave(product_count, active_batches, expected_batches):
    products = [{"product_id": str(i)} for i in range(product_count)]

    batches = plan_detail_batches(
        products,
        active_batches=active_batches,
        base_batch_size=500,
    )

    assert len(batches) == expected_batches
    assert sum(len(batch["product_batch"]) for batch in batches) == product_count
    assert [batch["batch_index"] for batch in batches] == list(range(expected_batches))


def test_detail_xcom_file_metadata_expands_to_results(tmp_path):
    results_file = tmp_path / "detail_batch.json"
    results_file.write_text(
        """
        {
          "batch_index": 0,
          "result_count": 2,
          "results": [
            {"product_id": "p1", "status": "success_api", "detail": {"brand": "A"}},
            {"product_id": "p2", "status": "partial_needs_enrichment", "detail": {}}
          ]
        }
        """,
        encoding="utf-8",
    )
    target = []

    _extend_detail_results_from_xcom(
        target,
        {"xcom_payload_version": 2, "results_file": str(results_file), "result_count": 2},
        logger=type("Logger", (), {"warning": lambda *args: None, "debug": lambda *args: None})(),
    )

    assert [result["product_id"] for result in target] == ["p1", "p2"]


def test_empty_detail_xcom_file_metadata_is_ignored():
    target = []

    _extend_detail_results_from_xcom(
        target,
        {"xcom_payload_version": 2, "results_file": None, "result_count": 0},
        logger=type("Logger", (), {"warning": lambda *args: None, "debug": lambda *args: None})(),
    )

    assert target == []


def test_detail_concurrency_is_clamped(monkeypatch):
    values = {
        "TIKI_DETAIL_MAX_CONCURRENT_TASKS": "99",
        "TIKI_DETAIL_CONCURRENCY_MIN": "4",
        "TIKI_DETAIL_CONCURRENCY_MAX": "10",
    }

    def fake_get_int_setting(name, use_airflow=True):
        return int(values[name])

    monkeypatch.setattr(
        "tiki_crawl_products_v2.tasks.detail_new.get_int_setting", fake_get_int_setting
    )

    logger = type(
        "Logger",
        (),
        {
            "warning": lambda *args: None,
            "info": lambda *args: None,
        },
    )()

    assert _get_detail_concurrency(logger) == 10


def test_resolve_product_load_mode_auto_threshold():
    assert _resolve_product_load_mode("auto", product_count=999, threshold=1000) == "safe"
    assert _resolve_product_load_mode("auto", product_count=1000, threshold=1000) == "optimized"
    assert _resolve_product_load_mode("traditional", product_count=2000, threshold=1000) == "safe"
    assert _resolve_product_load_mode("bulk", product_count=1, threshold=1000) == "optimized"


def test_select_categories_for_product_refs_keeps_referenced_non_leaf_and_parents():
    categories = [
        {"name": "Root", "url": "https://tiki.vn/root/c1", "parent_url": None},
        {
            "name": "Parent",
            "url": "https://tiki.vn/parent/c2",
            "parent_url": "https://tiki.vn/root/c1",
        },
        {
            "name": "Child",
            "url": "https://tiki.vn/child/c3",
            "parent_url": "https://tiki.vn/parent/c2",
        },
        {
            "name": "Unrelated",
            "url": "https://tiki.vn/unrelated/c4",
            "parent_url": "https://tiki.vn/root/c1",
        },
    ]

    selected = _select_categories_for_product_refs(categories, {"c2"})

    assert [cat["category_id"] for cat in selected] == ["c2", "c1"]


def test_resolve_current_product_file_does_not_use_stale_fallback_when_transform_xcom_unusable(
    tmp_path, monkeypatch
):
    current_file = tmp_path / "products_transformed.json"
    current_file.write_text('{"products": [{"product_id": "p1"}]}', encoding="utf-8")

    stale_dir = tmp_path / "processed"
    stale_dir.mkdir()
    stale_file = stale_dir / "merged_products.json"
    stale_file.write_text(
        '{"products": [{"product_id": "p2", "category_id": "c123"}]}',
        encoding="utf-8",
    )

    class FakeTi:
        def xcom_pull(self, task_ids):
            if task_ids == "transform_and_load.transform_products":
                return {"transformed_file": str(current_file)}
            return None

    logger = type(
        "Logger",
        (),
        {
            "info": lambda *args: None,
            "warning": lambda *args: None,
        },
    )()
    monkeypatch.setattr(dag_loader, "DATA_DIR", tmp_path)

    with pytest.raises(ValueError, match="does not contain category refs"):
        dag_loader._resolve_current_product_file({"ti": FakeTi()}, logger)
