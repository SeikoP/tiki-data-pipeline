"""Airflow DAG that prepares category artifacts before the main Tiki crawl DAG is run."""

from __future__ import annotations

import json
from contextlib import chdir
from pathlib import Path
from typing import Any

from tiki_crawl_products_v2.bootstrap import DAG, DAG_CONFIG, DATA_DIR, PythonOperator


def _validate_json_artifact(path: Path, expected_type: type) -> int:
    """Validate a non-empty JSON artifact and return its record count."""
    if not path.exists():
        raise RuntimeError(f"JSON artifact does not exist: {path}")

    try:
        with path.open(encoding="utf-8") as file:
            payload = json.load(file)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"Cannot read valid JSON artifact {path}: {exc}") from exc

    if type(payload) is not expected_type:
        raise RuntimeError(
            f"JSON artifact {path} must contain {expected_type.__name__}, "
            f"got {type(payload).__name__}"
        )
    if not payload:
        raise RuntimeError(f"JSON artifact is empty: {path}")

    return len(payload)


def crawl_category_tree_task(**_context: Any) -> dict[str, Any]:
    """Run the category crawler and return metadata for its validated output."""
    from pipelines.crawl.crawl_categories_optimized import main

    output_path = DATA_DIR / "raw" / "categories_recursive_optimized.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with chdir(DATA_DIR.parent):
        crawl_summary = main()

    if not isinstance(crawl_summary, dict):
        raise RuntimeError("Category crawler did not return a valid run summary")

    counter_fields = ("total_success", "total_failed", "total_categories")
    if any(
        type(crawl_summary.get(field)) is not int or crawl_summary[field] < 0
        for field in counter_fields
    ):
        raise RuntimeError("Category crawler did not return a valid run summary")

    reported_output = crawl_summary.get("output_file")
    if not isinstance(reported_output, str) or not reported_output.strip():
        raise RuntimeError("Category crawler did not return a valid run summary")
    reported_path = Path(reported_output)
    if not reported_path.is_absolute():
        reported_path = DATA_DIR.parent / reported_path
    if reported_path.resolve() != output_path.resolve():
        raise RuntimeError("Category crawler did not return a valid run summary")

    if crawl_summary["total_success"] < 1:
        raise RuntimeError("No category crawl succeeded")

    return {
        "output_file": str(output_path),
        "record_count": _validate_json_artifact(output_path, list),
    }


def build_category_hierarchy_task(**_context: Any) -> dict[str, Any]:
    """Build the category hierarchy and return metadata for its validated output."""
    from tools.build_category_hierarchy import build_category_hierarchy

    output_path = DATA_DIR / "raw" / "category_hierarchy_map.json"
    with chdir(DATA_DIR.parent):
        hierarchy = build_category_hierarchy()

    if not hierarchy:
        raise RuntimeError("Category builder returned an empty hierarchy")

    return {
        "output_file": str(output_path),
        "record_count": _validate_json_artifact(output_path, dict),
    }


dag_config = dict(DAG_CONFIG or {})
dag_config.update(
    {
        "dag_id": "tiki_prepare_categories",
        "description": "Prepare category tree artifacts before the Tiki product crawl",
        "schedule": None,
        "catchup": False,
        "tags": ["tiki", "crawl", "categories", "manual"],
        "max_active_runs": 1,
    }
)

with DAG(**dag_config) as dag:
    task_crawl_category_tree = PythonOperator(
        task_id="crawl_category_tree",
        python_callable=crawl_category_tree_task,
        doc_md=(
            "Crawl the configured Tiki root categories and validate the non-empty category tree."
        ),
    )

    task_build_category_hierarchy = PythonOperator(
        task_id="build_category_hierarchy",
        python_callable=build_category_hierarchy_task,
        doc_md="Build and validate the category parent/root hierarchy map.",
    )

    task_crawl_category_tree >> task_build_category_hierarchy
