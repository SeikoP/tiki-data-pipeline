from __future__ import annotations

import logging
import sys
from datetime import timedelta as datetime_timedelta
from pathlib import Path
from typing import Any

try:
    from tiki_crawl_products_v2.bootstrap import DAG, DAG_CONFIG, PythonOperator, timedelta
except ImportError:
    current_dir = Path(__file__).resolve().parent
    if str(current_dir) not in sys.path:
        sys.path.insert(0, str(current_dir))
    from tiki_crawl_products_v2.bootstrap import DAG, DAG_CONFIG, PythonOperator, timedelta

from tiki_assets import TIKI_CORE_TABLES_ASSET

logger = logging.getLogger(__name__)

if DAG is None:

    class DAG:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def __enter__(self) -> DAG:
            return self

        def __exit__(self, *args: Any) -> None:
            pass


if PythonOperator is None:

    class PythonOperator:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.output = None

        def __rshift__(self, other: Any) -> Any:
            return other

        def __lshift__(self, other: Any) -> PythonOperator:
            return self

        def __rrshift__(self, other: Any) -> PythonOperator:
            return self

        def __rlshift__(self, other: Any) -> Any:
            return other


if timedelta is None:
    timedelta = datetime_timedelta


def _analytics_schema() -> str:
    from common.settings import get_setting

    return str(get_setting("TIKI_ANALYTICS_SCHEMA"))


def _db_cursor():
    from pipelines.load.db_pool import get_db_pool, initialize_db_pool

    initialize_db_pool()
    return get_db_pool().get_cursor(commit=True)


def resolve_analytics_context(**context: Any) -> dict[str, Any]:
    """Resolve DAG run context for analytics refresh."""
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    crawl_run_id = conf.get("crawl_run_id")
    schema = _analytics_schema()
    return {"schema": schema, "crawl_run_id": crawl_run_id}


def ensure_schema_task(**context: Any) -> dict[str, Any]:
    from pipelines.analytics.normalizer import ensure_analytics_schema

    schema = _analytics_schema()
    with _db_cursor() as cursor:
        result = ensure_analytics_schema(cursor, schema)
    logger.info("Ensured analytics schema %s", schema)
    return result


def _run_refresh(refresh_name: str) -> dict[str, Any]:
    schema = _analytics_schema()
    from pipelines.analytics import normalizer

    refresh_func = getattr(normalizer, refresh_name)
    with _db_cursor() as cursor:
        result = refresh_func(cursor, schema)
    logger.info("Analytics refresh %s: %s", refresh_name, result)
    return result


dag_config = dict(DAG_CONFIG or {})
dag_config.update(
    {
        "dag_id": "tiki_analytics_normalize",
        "description": "Normalize loaded Tiki crawl data into analytics-ready marts",
        "schedule": [TIKI_CORE_TABLES_ASSET],
        "catchup": False,
        "tags": ["tiki", "analytics", "graduation", "power-bi"],
    }
)

with DAG(**dag_config) as dag:
    task_resolve_context = PythonOperator(
        task_id="resolve_analytics_context",
        python_callable=resolve_analytics_context,
        execution_timeout=timedelta(minutes=1),
        doc_md="Resolve analytics schema and optional crawl_run_id from dag_run.conf.",
    )

    task_ensure_schema = PythonOperator(
        task_id="ensure_analytics_schema",
        python_callable=ensure_schema_task,
        execution_timeout=timedelta(minutes=2),
        pool="fast_pool",
        doc_md="Create analytics schema and mart tables if they do not exist.",
    )

    task_refresh_dimensions = PythonOperator(
        task_id="refresh_dimensions",
        python_callable=lambda **context: _run_refresh("refresh_dimensions"),
        execution_timeout=timedelta(minutes=10),
        pool="fast_pool",
        doc_md="Refresh category, seller, and product dimensions.",
    )

    task_refresh_product_daily = PythonOperator(
        task_id="refresh_product_daily_fact",
        python_callable=lambda **context: _run_refresh("refresh_product_daily_fact"),
        execution_timeout=timedelta(minutes=20),
        pool="fast_pool",
        doc_md="Refresh product daily fact snapshots.",
    )

    task_refresh_category_daily = PythonOperator(
        task_id="refresh_category_daily_fact",
        python_callable=lambda **context: _run_refresh("refresh_category_daily_fact"),
        execution_timeout=timedelta(minutes=10),
        pool="fast_pool",
        doc_md="Refresh category daily aggregate facts.",
    )

    task_refresh_seller_daily = PythonOperator(
        task_id="refresh_seller_daily_fact",
        python_callable=lambda **context: _run_refresh("refresh_seller_daily_fact"),
        execution_timeout=timedelta(minutes=10),
        pool="fast_pool",
        doc_md="Refresh seller daily aggregate facts.",
    )

    task_refresh_price_change = PythonOperator(
        task_id="refresh_price_change_fact",
        python_callable=lambda **context: _run_refresh("refresh_price_change_fact"),
        execution_timeout=timedelta(minutes=10),
        pool="fast_pool",
        doc_md="Refresh price-change fact events.",
    )

    task_refresh_crawl_quality = PythonOperator(
        task_id="refresh_crawl_quality_fact",
        python_callable=lambda **context: _run_refresh("refresh_crawl_quality_fact"),
        execution_timeout=timedelta(minutes=10),
        pool="fast_pool",
        doc_md="Refresh crawl quality fact rows.",
    )

    task_refresh_analysis_ready = PythonOperator(
        task_id="refresh_product_analysis_ready",
        python_callable=lambda **context: _run_refresh("refresh_product_analysis_ready"),
        execution_timeout=timedelta(minutes=10),
        pool="fast_pool",
        doc_md="Refresh filtered product rows for the main Power BI dashboard.",
    )

    (
        task_resolve_context
        >> task_ensure_schema
        >> task_refresh_dimensions
        >> task_refresh_product_daily
    )
    (
        task_refresh_product_daily
        >> [
            task_refresh_category_daily,
            task_refresh_seller_daily,
            task_refresh_price_change,
            task_refresh_crawl_quality,
        ]
        >> task_refresh_analysis_ready
    )
