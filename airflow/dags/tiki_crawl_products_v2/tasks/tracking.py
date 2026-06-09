from __future__ import annotations

from tiki_crawl_products_v2.bootstrap import Any, get_logger
from tiki_crawl_products_v2.tasks.common import _fix_sys_path_for_pipelines_import  # noqa: F401

try:
    from airflow.sdk.exceptions import AirflowSkipException
except Exception:
    try:
        from airflow.exceptions import AirflowSkipException
    except Exception:

        class AirflowSkipException(Exception):
            """Fallback for local tests without a usable Airflow runtime."""


def _context_ids(context: dict[str, Any]) -> tuple[str | None, str | None]:
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    dag_id = getattr(dag, "dag_id", None) or getattr(dag, "id", None)
    run_id = getattr(dag_run, "run_id", None)
    return dag_id, run_id


def _xcom_dict(context: dict[str, Any], task_ids: list[str]) -> dict[str, Any] | None:
    ti = context.get("ti")
    if not ti:
        return None
    for task_id in task_ids:
        try:
            value = ti.xcom_pull(task_ids=task_id)
        except Exception:
            continue
        if isinstance(value, dict):
            return value
    return None


def start_crawl_run(**context) -> dict[str, Any]:
    logger = get_logger(context)
    from common.pipeline_tracking import start_crawl_run as _start_crawl_run

    dag_id, run_id = _context_ids(context)
    crawl_run_id = _start_crawl_run(airflow_dag_id=dag_id, airflow_run_id=run_id)
    logger.info("🧾 Started crawl_run_id=%s dag_id=%s run_id=%s", crawl_run_id, dag_id, run_id)
    return {"crawl_run_id": crawl_run_id, "status": "running"}


def finish_crawl_run(**context) -> dict[str, Any]:
    logger = get_logger(context)
    from common.pipeline_tracking import finish_crawl_run as _finish_crawl_run

    run_info = _xcom_dict(context, ["start_crawl_run", "pipeline_tracking.start_crawl_run"])
    crawl_run_id = (
        int(run_info["crawl_run_id"]) if run_info and run_info.get("crawl_run_id") else None
    )
    if not crawl_run_id:
        logger.warning("No crawl_run_id found; skipping finish_crawl_run")
        return {"status": "skipped", "reason": "missing crawl_run_id"}

    merge_result = _xcom_dict(context, ["process_and_save.merge_products", "merge_products"]) or {}
    transform_result = (
        _xcom_dict(context, ["transform_and_load.transform_products", "transform_products"]) or {}
    )
    load_result = _xcom_dict(context, ["transform_and_load.load_products", "load_products"]) or {}

    merge_stats = merge_result.get("stats") or {}
    load_stats = load_result.get("load_stats") or {}
    raw_count = int(merge_stats.get("unique_products") or merge_result.get("product_count") or 0)
    transformed_count = int(transform_result.get("transformed_count") or 0)
    loaded_count = int(load_stats.get("db_loaded") or 0)
    failed_count = int(load_stats.get("failed_count") or 0)

    status = "success" if loaded_count > 0 and failed_count == 0 else "failed"
    error_message = None
    if status == "failed":
        errors = load_stats.get("errors") or []
        error_message = "; ".join(map(str, errors[:3])) or "load did not complete successfully"

    _finish_crawl_run(
        crawl_run_id=crawl_run_id,
        status=status,
        total_raw_items=raw_count,
        total_transformed_items=transformed_count,
        total_loaded_items=loaded_count,
        error_message=error_message,
    )
    logger.info(
        "🧾 Finished crawl_run_id=%s status=%s loaded=%s", crawl_run_id, status, loaded_count
    )
    return {
        "crawl_run_id": crawl_run_id,
        "status": status,
        "total_raw_items": raw_count,
        "total_transformed_items": transformed_count,
        "total_loaded_items": loaded_count,
        "error_message": error_message,
    }


def publish_core_tables_asset(**context) -> dict[str, Any]:
    """Publish the core-table asset only after a successful crawl/load run."""
    logger = get_logger(context)
    from tiki_assets import TIKI_CORE_TABLES_ASSET_URI

    finish_info = _xcom_dict(
        context,
        [
            "maintenance_and_reports.finish_crawl_run",
            "finish_crawl_run",
        ],
    )
    if not finish_info or finish_info.get("status") != "success":
        status = finish_info.get("status") if finish_info else "missing"
        raise AirflowSkipException(f"Core table asset not published because crawl status={status}")

    crawl_run_id = finish_info.get("crawl_run_id")
    logger.info(
        "📦 Published core table asset %s for crawl_run_id=%s",
        TIKI_CORE_TABLES_ASSET_URI,
        crawl_run_id,
    )
    return {
        "status": "success",
        "crawl_run_id": crawl_run_id,
        "asset_uri": TIKI_CORE_TABLES_ASSET_URI,
    }
