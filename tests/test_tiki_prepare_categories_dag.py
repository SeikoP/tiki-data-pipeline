"""Tests for the manual category-preparation DAG."""

from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import tiki_prepare_categories as prepare_dag


@pytest.mark.parametrize(
    ("payload", "expected_type", "expected_count"),
    [
        ([{"url": "https://tiki.vn/c1"}], list, 1),
        ({"https://tiki.vn/c1": {"name": "Category"}}, dict, 1),
    ],
)
def test_validate_json_artifact_returns_record_count(
    tmp_path: Path,
    payload: object,
    expected_type: type,
    expected_count: int,
) -> None:
    output = tmp_path / "artifact.json"
    output.write_text(json.dumps(payload), encoding="utf-8")

    assert prepare_dag._validate_json_artifact(output, expected_type) == expected_count


@pytest.mark.parametrize(
    ("content", "expected_type"),
    [
        ("not-json", list),
        ("[]", list),
        ("{}", dict),
        ('{"unexpected": true}', list),
        ('["unexpected"]', dict),
    ],
)
def test_validate_json_artifact_rejects_invalid_output(
    tmp_path: Path,
    content: str,
    expected_type: type,
) -> None:
    output = tmp_path / "artifact.json"
    output.write_text(content, encoding="utf-8")

    with pytest.raises(RuntimeError, match="artifact.json"):
        prepare_dag._validate_json_artifact(output, expected_type)


def test_validate_json_artifact_rejects_missing_file(tmp_path: Path) -> None:
    output = tmp_path / "missing.json"

    with pytest.raises(RuntimeError, match="missing.json"):
        prepare_dag._validate_json_artifact(output, list)


def test_crawl_category_tree_task_returns_file_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pipelines.crawl import crawl_categories_optimized

    data_dir = tmp_path / "data"
    output = data_dir / "raw" / "categories_recursive_optimized.json"
    original_cwd = Path.cwd()

    def fake_crawl() -> dict[str, int | str]:
        assert Path.cwd() == tmp_path
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text('[{"url": "https://tiki.vn/c1"}]', encoding="utf-8")
        return {
            "total_success": 1,
            "total_failed": 0,
            "total_categories": 1,
            "output_file": "data/raw/categories_recursive_optimized.json",
        }

    monkeypatch.setattr(prepare_dag, "DATA_DIR", data_dir, raising=False)
    monkeypatch.setattr(crawl_categories_optimized, "main", fake_crawl)

    result = prepare_dag.crawl_category_tree_task()

    assert result == {"output_file": str(output), "record_count": 1}
    assert Path.cwd() == original_cwd


def test_crawl_category_tree_task_rejects_all_failed_crawls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pipelines.crawl import crawl_categories_optimized

    data_dir = tmp_path / "data"
    output = data_dir / "raw" / "categories_recursive_optimized.json"

    def fake_crawl() -> dict[str, int | str]:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            '[{"url": "https://tiki.vn/c1883"}, {"url": "https://tiki.vn/c4221"}]',
            encoding="utf-8",
        )
        return {
            "total_success": 0,
            "total_failed": 2,
            "total_categories": 0,
            "output_file": "data/raw/categories_recursive_optimized.json",
        }

    monkeypatch.setattr(prepare_dag, "DATA_DIR", data_dir, raising=False)
    monkeypatch.setattr(crawl_categories_optimized, "main", fake_crawl)

    with pytest.raises(RuntimeError, match="No category crawl succeeded"):
        prepare_dag.crawl_category_tree_task()


@pytest.mark.parametrize(
    "crawl_summary",
    [
        None,
        {},
        {"total_success": "1"},
        {"total_success": 1},
        {
            "total_success": 1,
            "total_failed": "0",
            "total_categories": 1,
            "output_file": "data/raw/categories_recursive_optimized.json",
        },
        {
            "total_success": 1,
            "total_failed": 0,
            "total_categories": -1,
            "output_file": "data/raw/categories_recursive_optimized.json",
        },
        {
            "total_success": 1,
            "total_failed": 0,
            "total_categories": 1,
            "output_file": "data/raw/stale_categories.json",
        },
    ],
)
def test_crawl_category_tree_task_rejects_invalid_crawl_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    crawl_summary: object,
) -> None:
    from pipelines.crawl import crawl_categories_optimized

    data_dir = tmp_path / "data"
    output = data_dir / "raw" / "categories_recursive_optimized.json"

    def fake_crawl() -> object:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text('[{"url": "https://tiki.vn/c1"}]', encoding="utf-8")
        return crawl_summary

    monkeypatch.setattr(prepare_dag, "DATA_DIR", data_dir, raising=False)
    monkeypatch.setattr(crawl_categories_optimized, "main", fake_crawl)

    with pytest.raises(RuntimeError, match="valid run summary"):
        prepare_dag.crawl_category_tree_task()


def test_build_category_hierarchy_task_returns_file_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tools import build_category_hierarchy

    data_dir = tmp_path / "data"
    output = data_dir / "raw" / "category_hierarchy_map.json"
    original_cwd = Path.cwd()
    expected_hierarchy = {"https://tiki.vn/c1": {"name": "Category"}}

    def fake_build() -> dict[str, dict[str, str]]:
        assert Path.cwd() == tmp_path
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(expected_hierarchy), encoding="utf-8")
        return expected_hierarchy

    monkeypatch.setattr(prepare_dag, "DATA_DIR", data_dir, raising=False)
    monkeypatch.setattr(build_category_hierarchy, "build_category_hierarchy", fake_build)

    result = prepare_dag.build_category_hierarchy_task()

    assert result == {"output_file": str(output), "record_count": 1}
    assert Path.cwd() == original_cwd


def test_build_category_hierarchy_task_rejects_empty_builder_result(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from tools import build_category_hierarchy

    original_cwd = Path.cwd()
    monkeypatch.setattr(prepare_dag, "DATA_DIR", tmp_path / "data", raising=False)
    monkeypatch.setattr(build_category_hierarchy, "build_category_hierarchy", lambda: {})

    with pytest.raises(RuntimeError, match="empty hierarchy"):
        prepare_dag.build_category_hierarchy_task()

    assert Path.cwd() == original_cwd


def test_prepare_categories_dag_is_manual_and_sequential() -> None:
    dag = prepare_dag.dag

    assert dag.dag_id == "tiki_prepare_categories"
    assert dag.schedule is None
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.task_ids == ["crawl_category_tree", "build_category_hierarchy"]
    assert dag.get_task("build_category_hierarchy").upstream_task_ids == {"crawl_category_tree"}


def test_prepare_categories_dag_parse_keeps_task_modules_lazy() -> None:
    env = os.environ.copy()
    env["PREPARE_DAG_FILE"] = str(Path(prepare_dag.__file__))
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import importlib.util, os, sys; "
                "spec = importlib.util.spec_from_file_location("
                "'isolated_prepare_dag', os.environ['PREPARE_DAG_FILE']); "
                "module = importlib.util.module_from_spec(spec); "
                "spec.loader.exec_module(module); "
                "assert 'pipelines.crawl.crawl_categories_optimized' not in sys.modules; "
                "assert 'tools.build_category_hierarchy' not in sys.modules"
            ),
        ],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_airflow_dagbag_discovers_prepare_categories_dag() -> None:
    if importlib.util.find_spec("airflow") is None:
        pytest.skip("Airflow is not installed in the local test environment")

    env = os.environ.copy()
    env["PREPARE_DAG_FILE"] = str(Path(prepare_dag.__file__))
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import os; "
                "from airflow.models import DagBag; "
                "bag = DagBag(dag_folder=os.environ['PREPARE_DAG_FILE'], "
                "include_examples=False); "
                "assert not bag.import_errors, bag.import_errors; "
                "assert 'tiki_prepare_categories' in bag.dag_ids, bag.dag_ids"
            ),
        ],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stdout + result.stderr
