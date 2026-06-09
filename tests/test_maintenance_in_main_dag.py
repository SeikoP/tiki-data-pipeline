from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).parent.parent
FAST_DAG = ROOT / "airflow" / "dags" / "tiki_crawl_products_v2" / "tiki_crawl_fast.py"
MAINTENANCE_DAG = ROOT / "airflow" / "dags" / "tiki_maintenance.py"


def _task_ids(path: Path) -> set[str]:
    module = ast.parse(path.read_text(encoding="utf-8"))
    task_ids: set[str] = set()
    for node in ast.walk(module):
        if isinstance(node, ast.keyword) and node.arg == "task_id":
            value = node.value
            if isinstance(value, ast.Constant) and isinstance(value.value, str):
                task_ids.add(value.value)
    return task_ids


def test_maintenance_tasks_run_in_fast_dag():
    assert not MAINTENANCE_DAG.exists()

    fast_task_ids = _task_ids(FAST_DAG)
    assert {
        "reconcile_categories",
        "cleanup_categories",
        "cleanup_old_history",
        "cleanup_redis_cache",
        "backup_database",
        "finish_crawl_run",
        "publish_core_tables_asset",
    }.issubset(fast_task_ids)
    assert "trigger_tiki_maintenance" not in fast_task_ids
