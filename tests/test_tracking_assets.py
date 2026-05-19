from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).parent.parent
SRC = ROOT / "src"
DAGS = ROOT / "airflow" / "dags"
for path in (SRC, DAGS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))


class _TaskInstance:
    def __init__(self, value):
        self.value = value

    def xcom_pull(self, task_ids):
        return self.value


def test_publish_core_tables_asset_returns_asset_metadata_on_success():
    from tiki_crawl_products_v2.tasks.tracking import publish_core_tables_asset

    result = publish_core_tables_asset(
        ti=_TaskInstance({"status": "success", "crawl_run_id": 42}),
        dag=SimpleNamespace(dag_id="tiki_crawl_fast"),
    )

    assert result == {
        "status": "success",
        "crawl_run_id": 42,
        "asset_uri": "postgres://postgres:5432/tiki/public/core_product_tables",
    }


def test_publish_core_tables_asset_skips_when_crawl_failed():
    from tiki_crawl_products_v2.tasks.tracking import (
        AirflowSkipException,
        publish_core_tables_asset,
    )

    with pytest.raises(AirflowSkipException):
        publish_core_tables_asset(
            ti=_TaskInstance({"status": "failed", "crawl_run_id": 42}),
            dag=SimpleNamespace(dag_id="tiki_crawl_fast"),
        )
