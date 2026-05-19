import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(REPO_ROOT / "src"))
sys.path.insert(0, str(REPO_ROOT / "airflow" / "dags"))

from tiki_crawl_products_v2.tasks import loader as dag_loader  # noqa: E402
from tiki_crawl_products_v2.tasks.category import (  # noqa: E402
    _parse_segment_quotas,
    _select_categories_by_segment_quota,
    _window_categories_for_run,
)
from tiki_crawl_products_v2.tasks.detail import (  # noqa: E402
    _extend_detail_results_from_xcom,
    _select_products_for_detail,
    _validate_detail_results_available,
)
from tiki_crawl_products_v2.tasks.detail_new import (  # noqa: E402
    _get_detail_concurrency,
    _summarize_detail_metrics,
)
from tiki_crawl_products_v2.tasks.loader import _resolve_product_load_mode  # noqa: E402
from tiki_crawl_products_v2.tasks.product import (  # noqa: E402
    _resolve_detail_output_file,
    _validate_merge_products_quality,
)

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


def test_detail_request_delay_staggers_by_configured_interval():
    from tiki_crawl_products_v2.tasks.detail_new import _detail_request_delay

    assert _detail_request_delay(index=0, rate_limit_delay=0.05) == 0
    assert _detail_request_delay(index=3, rate_limit_delay=0.05) == pytest.approx(0.15)


def test_summarize_detail_metrics_reports_latency_and_reasons():
    results = [
        {
            "product_id": "p1",
            "status": "success_api",
            "metrics": {"duration_ms": 100, "api_attempts": 1, "api_status": 200},
        },
        {
            "product_id": "p2",
            "status": "failed",
            "metrics": {
                "duration_ms": 400,
                "api_attempts": 3,
                "api_status": "200 (HTML instead of JSON)",
                "api_reason": "api_html_instead_of_json",
            },
        },
        {
            "product_id": "p3",
            "status": "failed",
            "metrics": {
                "duration_ms": 250,
                "api_attempts": 2,
                "api_status": 429,
                "api_reason": "api_retriable_http_status",
            },
        },
    ]

    summary = _summarize_detail_metrics(results, slowest_limit=2)

    assert summary["count"] == 3
    assert summary["avg_ms"] == pytest.approx(250)
    assert summary["p50_ms"] == 250
    assert summary["p95_ms"] == 400
    assert summary["max_ms"] == 400
    assert summary["failure_reasons"] == {
        "api_html_instead_of_json": 1,
        "api_retriable_http_status": 1,
    }
    assert summary["api_statuses"] == {"200": 1, "200 (HTML instead of JSON)": 1, "429": 1}
    assert [item["product_id"] for item in summary["slowest"]] == ["p2", "p3"]


def test_resolve_product_load_mode_auto_threshold():
    assert _resolve_product_load_mode("auto", product_count=999, threshold=1000) == "safe"
    assert _resolve_product_load_mode("auto", product_count=1000, threshold=1000) == "optimized"
    assert _resolve_product_load_mode("traditional", product_count=2000, threshold=1000) == "safe"
    assert _resolve_product_load_mode("bulk", product_count=1, threshold=1000) == "optimized"


def test_window_categories_for_run_rotates_by_run_key():
    categories = [{"category_id": f"c{i}"} for i in range(1, 7)]

    first = _window_categories_for_run(categories, max_categories=2, run_key="1")
    second = _window_categories_for_run(categories, max_categories=2, run_key="2")

    assert [cat["category_id"] for cat in first] == ["c1", "c2"]
    assert [cat["category_id"] for cat in second] == ["c3", "c4"]


def test_parse_segment_quotas_accepts_comma_separated_pairs():
    assert _parse_segment_quotas("kitchen_cooking:30,bathroom:20, bad:0,invalid") == {
        "kitchen_cooking": 30,
        "bathroom": 20,
    }


def test_select_categories_by_segment_quota_balances_target_segments():
    categories = [
        {
            "category_id": "k1",
            "name": "Bình đựng nước",
            "category_path": ["Nhà cửa đời sống", "Đồ dùng phòng ăn", "Bình đựng nước"],
        },
        {
            "category_id": "k2",
            "name": "Dụng cụ nhà bếp",
            "category_path": ["Nhà cửa đời sống", "Dụng cụ nhà bếp"],
        },
        {
            "category_id": "b1",
            "name": "Kệ nhà tắm",
            "category_path": ["Nhà cửa đời sống", "Nhà tắm"],
        },
        {
            "category_id": "x1",
            "name": "Amply",
            "category_path": ["Nhà cửa đời sống", "Nhạc cụ", "Amply"],
        },
    ]

    selected = _select_categories_by_segment_quota(
        categories,
        segment_quotas={"kitchen_cooking": 1, "bathroom": 1, "home_comfort": 1},
        max_categories=10,
        run_key="1",
    )

    assert [cat["category_id"] for cat in selected] == ["k1", "b1", "k2"]
    assert [cat["home_segment"] for cat in selected] == [
        "kitchen_cooking",
        "bathroom",
        "kitchen_cooking",
    ]


def test_select_categories_by_segment_quota_refills_underfilled_segments():
    categories = [
        {
            "category_id": "k1",
            "name": "Dụng cụ nhà bếp 1",
            "category_path": ["Nhà cửa đời sống", "Dụng cụ nhà bếp", "Nồi"],
        },
        {
            "category_id": "k2",
            "name": "Dụng cụ nhà bếp 2",
            "category_path": ["Nhà cửa đời sống", "Dụng cụ nhà bếp", "Chảo"],
        },
        {
            "category_id": "k3",
            "name": "Dụng cụ nhà bếp 3",
            "category_path": ["Nhà cửa đời sống", "Dụng cụ nhà bếp", "Dao"],
        },
        {
            "category_id": "b1",
            "name": "Kệ nhà tắm",
            "category_path": ["Nhà cửa đời sống", "Nhà tắm", "Kệ nhà tắm"],
        },
    ]

    selected = _select_categories_by_segment_quota(
        categories,
        segment_quotas={"kitchen_cooking": 1, "bathroom": 1, "cleaning_homecare": 2},
        max_categories=4,
        run_key="1",
    )

    assert [cat["category_id"] for cat in selected] == ["k1", "b1", "k2", "k3"]


def test_select_products_for_detail_applies_effective_limit_and_skips_recent_db_hits():
    products = [
        {"product_id": "p1", "url": "https://tiki.vn/p1.html", "name": "P1"},
        {"product_id": "p2", "url": "https://tiki.vn/p2.html", "name": "P2"},
        {"product_id": "p3", "url": "https://tiki.vn/p3.html", "name": "P3"},
    ]

    selected = _select_products_for_detail(
        products,
        existing_product_ids_in_db={"p1"},
        products_per_day=3,
        max_products=1,
        force_crawl=False,
    )

    assert [item["product_id"] for item in selected] == ["p2"]
    assert selected[0]["product"] == products[1]


def test_select_products_for_detail_balances_analysis_segments():
    products = [
        {
            "product_id": "k1",
            "url": "https://tiki.vn/k1.html",
            "name": "K1",
            "category_path": ["Nha Cua Doi Song", "Dụng cụ nhà bếp"],
        },
        {
            "product_id": "k2",
            "url": "https://tiki.vn/k2.html",
            "name": "K2",
            "category_path": ["Nha Cua Doi Song", "Dụng cụ nhà bếp"],
        },
        {
            "product_id": "k3",
            "url": "https://tiki.vn/k3.html",
            "name": "K3",
            "category_path": ["Nha Cua Doi Song", "Dụng cụ nhà bếp"],
        },
        {
            "product_id": "b1",
            "url": "https://tiki.vn/b1.html",
            "name": "B1",
            "category_path": ["Nha Cua Doi Song", "Đồ dùng và thiết bị nhà tắm"],
        },
        {
            "product_id": "d1",
            "url": "https://tiki.vn/d1.html",
            "name": "D1",
            "category_path": ["Nha Cua Doi Song", "Trang trí nhà cửa"],
        },
    ]

    selected = _select_products_for_detail(
        products,
        existing_product_ids_in_db=set(),
        products_per_day=4,
        max_products=0,
        force_crawl=False,
    )

    assert [item["product_id"] for item in selected] == ["k1", "b1", "d1", "k2"]


def test_select_products_for_detail_force_crawl_ignores_recent_db_hits():
    products = [
        {"product_id": "p1", "url": "https://tiki.vn/p1.html", "name": "P1"},
        {"product_id": "p2", "url": "https://tiki.vn/p2.html", "name": "P2"},
    ]

    selected = _select_products_for_detail(
        products,
        existing_product_ids_in_db={"p1"},
        products_per_day=2,
        max_products=0,
        force_crawl=True,
    )

    assert [item["product_id"] for item in selected] == ["p1", "p2"]


def test_validate_merge_products_quality_rejects_empty_successful_crawl():
    with pytest.raises(RuntimeError, match="No products were merged"):
        _validate_merge_products_quality(
            {"total_categories": 2, "success_categories": 2, "failed_categories": 0},
            unique_product_count=0,
        )


def test_validate_merge_products_quality_rejects_all_failed_categories():
    with pytest.raises(RuntimeError, match="No category crawl succeeded"):
        _validate_merge_products_quality(
            {"total_categories": 2, "success_categories": 0, "failed_categories": 2},
            unique_product_count=0,
        )


def test_resolve_detail_output_file_rejects_invalid_upstream_xcom(tmp_path):
    default_file = tmp_path / "products_with_detail.json"
    default_file.write_text('{"products": [{"product_id": "stale"}]}', encoding="utf-8")

    with pytest.raises(ValueError, match="invalid output_file"):
        _resolve_detail_output_file({"output_file": None}, str(default_file))


def test_validate_detail_results_available_rejects_missing_mapped_results():
    with pytest.raises(RuntimeError, match="No detail results were available"):
        _validate_detail_results_available(
            expected_products_count=3,
            actual_crawl_count=2,
            detail_result_count=0,
        )


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
