from __future__ import annotations

from common.settings import get_float_setting, get_int_setting, get_setting
from tiki_crawl_products_v2.bootstrap import (  # crawl_product_detail_async,  <-- REMOVED
    Any,
    crawl_product_detail_with_driver,
    datetime,
    get_hierarchy_map,
    get_logger,
)
from tiki_crawl_products_v2.tasks.common import (
    atomic_write_file,
    get_selenium_driver_pool_class,
)


def _get_crawl_run_id(context: dict[str, Any]) -> int | None:
    ti = context.get("ti")
    if not ti:
        return None
    for task_id in ("start_crawl_run", "pipeline_tracking.start_crawl_run"):
        try:
            value = ti.xcom_pull(task_ids=task_id)
        except Exception:
            continue
        if isinstance(value, dict) and value.get("crawl_run_id"):
            return int(value["crawl_run_id"])
        if isinstance(value, int):
            return value
    return None


def _track_detail_results(
    results: list[dict[str, Any]], context: dict[str, Any], logger
) -> None:
    crawl_run_id = _get_crawl_run_id(context)
    if not crawl_run_id or not results:
        return
    try:
        from common.pipeline_tracking import upsert_crawl_tasks

        tasks = []
        for result in results:
            product_id = result.get("product_id")
            if not product_id:
                continue
            tasks.append(
                {
                    "crawl_run_id": crawl_run_id,
                    "task_key": f"detail:{product_id}",
                    "task_type": "detail",
                    "status": result.get("status") or "unknown",
                    "url": result.get("url"),
                    "retry_count": 0,
                    "error_message": result.get("error"),
                }
            )
        upsert_crawl_tasks(tasks)
    except Exception as tracking_error:
        logger.warning("⚠️ Không thể ghi audit detail tasks: %s", tracking_error)


def _get_detail_concurrency(logger) -> int:
    """Return clamped per-task async concurrency for fast detail crawl."""
    configured = get_int_setting("TIKI_DETAIL_MAX_CONCURRENT_TASKS")
    min_concurrency = get_int_setting("TIKI_DETAIL_CONCURRENCY_MIN")
    max_concurrency = get_int_setting("TIKI_DETAIL_CONCURRENCY_MAX")

    if min_concurrency > max_concurrency:
        logger.warning(
            "⚠️ TIKI_DETAIL_CONCURRENCY_MIN > TIKI_DETAIL_CONCURRENCY_MAX; swapping values"
        )
        min_concurrency, max_concurrency = max_concurrency, min_concurrency

    concurrency = max(min_concurrency, min(configured, max_concurrency))
    if concurrency != configured:
        logger.info(
            "ℹ️ Detail concurrency clamped from %s to %s (range %s-%s)",
            configured,
            concurrency,
            min_concurrency,
            max_concurrency,
        )
    return concurrency


def _write_detail_batch_results(
    results: list[dict[str, Any]], batch_index: int, context: dict[str, Any]
) -> str:
    """Persist detail results and return the file path kept in XCom metadata."""
    output_dir = get_setting("TIKI_DETAIL_BATCH_OUTPUT_DIR")
    run_id = "manual"
    ti = context.get("ti") or context.get("task_instance")
    if ti is not None:
        run_id = getattr(ti, "run_id", run_id)

    safe_run_id = (
        str(run_id).replace(":", "-").replace("/", "_").replace("\\", "_").replace("+", "_")
    )

    from pathlib import Path

    output_path = Path(output_dir) / safe_run_id / f"detail_batch_{batch_index:05d}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    atomic_write_file(
        str(output_path),
        {
            "batch_index": batch_index,
            "result_count": len(results),
            "written_at": datetime.now().isoformat(),
            "results": results,
        },
        **context,
    )
    return str(output_path)


async def _crawl_fast_internal(product_batch, logger, session=None):
    import asyncio

    # Setup aiohttp session
    own_session = session is None
    if own_session:
        try:
            import aiohttp

            from pipelines.crawl.config import (
                HTTP_CONNECTOR_LIMIT,
                HTTP_CONNECTOR_LIMIT_PER_HOST,
                HTTP_DNS_CACHE_TTL,
            )

            connector = aiohttp.TCPConnector(
                limit=HTTP_CONNECTOR_LIMIT,
                limit_per_host=HTTP_CONNECTOR_LIMIT_PER_HOST,
                ttl_dns_cache=HTTP_DNS_CACHE_TTL,
                force_close=False,
                enable_cleanup_closed=True,
            )
            timeout = aiohttp.ClientTimeout(total=20)
            session = aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
                },
            )

            # FORCE IMPORT DIRECTLY FROM SRC TO AVOID BOOTSTRAP ISSUES
            import sys

            if "/opt/airflow/src" not in sys.path:
                sys.path.append("/opt/airflow/src")
            try:
                from pipelines.crawl.crawl_products_detail import crawl_product_detail_async
            except ImportError:
                # Fallback local dev
                from src.pipelines.crawl.crawl_products_detail import crawl_product_detail_async
        except Exception as e:
            logger.error(f"❌ Failed to create session: {e}")
            return []

    rate_limit_delay = get_float_setting("TIKI_DETAIL_RATE_LIMIT_DELAY")
    max_concurrent = _get_detail_concurrency(logger)
    logger.info("⚙️ FAST detail async concurrency: %s", max_concurrent)
    semaphore = asyncio.Semaphore(max_concurrent)

    async def crawl_single(product_info):
        async with semaphore:
            product_id = product_info.get("product_id", "unknown")
            product_url = product_info.get("url", "")

            result = {
                "product_id": product_id,
                "url": product_url,
                "product": product_info.get("product", {}),
                "status": "failed",
                "error": None,
                "detail": None,
                "crawled_at": datetime.now().isoformat(),
            }

            try:
                # Force NO Selenium fallback
                detail = await crawl_product_detail_async(
                    product_url,
                    session=session,
                    use_selenium_fallback=False,
                    verbose=True,  # Enable Verbose Logging
                )

                if detail and isinstance(detail, dict):
                    result["detail"] = detail

                    # Logic: Needs Enrichment?
                    # Check for critical fields
                    has_brand = detail.get("brand") and detail.get("brand") != "Unknown"
                    has_seller = bool(detail.get("seller", {}).get("name"))
                    metadata = detail.get("_metadata") or {}
                    extraction_method = metadata.get("extraction_method", "unknown")

                    if not has_brand or not has_seller:
                        result["detail"]["needs_enrichment"] = True
                        result["detail"]["crawl_quality"] = "partial"
                        result["status"] = "partial_needs_enrichment"
                    else:
                        result["detail"]["needs_enrichment"] = False
                        result["detail"]["crawl_quality"] = "full"
                        if extraction_method == "api":
                            result["status"] = "success_api"
                        else:
                            result["status"] = "failed"
                            result["error"] = (
                                f"Unsupported detail extraction method: {extraction_method}"
                            )
                else:
                    # Failed to get structured data via API/HTML
                    # Mark as failed so it might be retried or picked up by enrichment if we insert a stub?
                    # Ideally we should insert a stub if we think it exists but failed to parse.
                    # But for now, just mark failed.
                    # Actually, if we want enrichment to pick it up, we might need to save it as 'failed' quality?
                    result["error"] = "Fast crawl failed"
                    result["status"] = "failed"

            except Exception as e:
                result["error"] = str(e)
                result["status"] = "failed"
                # LOG TO FILE
                try:
                    with open("/tmp/debug_crawl.log", "a") as f:
                        f.write(f"[{datetime.now()}] FAIL {product_id}: {str(e)}\n")
                except Exception:
                    pass

            return result

    # Create tasks with delay
    tasks = []
    for i, product in enumerate(product_batch):
        delay = i * rate_limit_delay / len(product_batch)

        async def task_with_delay(p, d):
            if d > 0:
                await asyncio.sleep(d)
            return await crawl_single(p)

        tasks.append(task_with_delay(product, delay))

    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Close session if we created it
    if own_session and session:
        await session.close()

    return results


def crawl_product_batch_fast(
    product_batch: list[dict[str, Any]] = None, batch_index: int = -1, **context
) -> dict[str, Any]:
    """
    Task: FAST Crawl - API & HTML only, No Selenium.
    Sets needs_enrichment=True if data is incomplete.
    """
    logger = get_logger(context)

    # Extract product_batch logic (same as original)
    if not product_batch:
        ti = context.get("ti")
        if ti and getattr(ti, "op_kwargs", {}):
            product_batch = ti.op_kwargs.get("product_batch")
        if not product_batch:
            product_batch = context.get("product_batch")

    if not product_batch:
        logger.warning(f"⚠️  Batch {batch_index} empty")
        return {
            "xcom_payload_version": 2,
            "batch_index": batch_index,
            "results_file": None,
            "result_count": 0,
            "stats": {"success": 0, "partial": 0, "failed": 0, "needs_enrichment": 0},
        }

    logger.info(f"🚀 FAST BATCH {batch_index}: {len(product_batch)} items")

    import asyncio

    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    results = loop.run_until_complete(_crawl_fast_internal(product_batch, logger))

    # Post-process: Skip direct DB write, let transform_and_load handle it
    saved_count = 0

    # Determine stats & Clean up results for XCom
    final_results = []
    success = 0
    partial = 0
    needs_enrich = 0
    failed = 0

    for r in results:
        if isinstance(r, Exception):
            failed += 1
            final_results.append(
                {
                    "product_id": None,
                    "url": None,
                    "status": "failed",
                    "error": str(r),
                    "detail": {},
                    "needs_enrichment": False,
                    "crawl_quality": "unknown",
                }
            )
            continue

        status = r.get("status")
        detail = r.get("detail") or {}
        is_success = status in {"success", "success_api"}

        if is_success:
            success += 1
        elif status == "partial_needs_enrichment":
            partial += 1
            needs_enrich += 1
        elif status == "failed":
            failed += 1

        # Full payload for XCom
        final_results.append(
            {
                "product_id": r.get("product_id"),
                "url": r.get("url"),
                "status": status,
                "error": r.get("error"),
                "detail": detail,  # Trả về detail qua XCom
                "needs_enrichment": detail.get("needs_enrichment", False),
                "crawl_quality": detail.get("crawl_quality", "unknown"),
            }
        )

    logger.info(
        "✅ FAST BATCH DONE: success=%s partial=%s failed=%s total=%s needs_enrichment=%s saved=%s",
        success,
        partial,
        failed,
        len(product_batch),
        needs_enrich,
        saved_count,
    )
    _track_detail_results(final_results, context, logger)
    results_file = _write_detail_batch_results(final_results, batch_index, context)
    return {
        "xcom_payload_version": 2,
        "batch_index": batch_index,
        "results_file": results_file,
        "result_count": len(final_results),
        "stats": {
            "success": success,
            "partial": partial,
            "failed": failed,
            "needs_enrichment": needs_enrich,
        },
    }


def crawl_product_batch_selenium(
    product_batch: list[dict[str, Any]] = None, batch_index: int = -1, **context
) -> list[dict[str, Any]]:
    """
    Task: ENRICHMENT Crawl - Selenium Only.
    Uses Driver Pool.
    """
    logger = get_logger(context)

    if not product_batch:
        ti = context.get("ti")
        if ti and getattr(ti, "op_kwargs", {}):
            product_batch = ti.op_kwargs.get("product_batch")
        if not product_batch:
            product_batch = context.get("product_batch")

    if not product_batch:
        return []

    logger.info(f"🐢 SELENIUM BATCH {batch_index}: {len(product_batch)} items")

    # Initialize Driver Pool
    DriverPool = get_selenium_driver_pool_class(logger)
    pool_size = get_int_setting("TIKI_DETAIL_POOL_SIZE")
    driver_pool = DriverPool(pool_size=pool_size, headless=True)

    results = []

    try:
        for product in product_batch:
            product_id = product.get("product_id")
            product_url = product.get("url")

            result = {
                "product_id": product_id,
                "url": product_url,
                "product": product.get("product", {}),
                "status": "failed",
                "detail": None,
                "crawled_at": datetime.now().isoformat(),
            }

            drv = None
            try:
                drv = driver_pool.get_driver()
                if drv:
                    html = crawl_product_detail_with_driver(
                        drv,
                        product_url,
                        save_html=False,
                        verbose=False,
                        timeout=90,
                        use_redis_cache=True,  # Cache hit is fine if it contains full data
                    )

                    if html:
                        hierarchy_map = get_hierarchy_map()
                        from tiki_crawl_products_v2.bootstrap import extract_product_detail

                        detail = extract_product_detail(
                            html, product_url, verbose=False, hierarchy_map=hierarchy_map
                        )

                        if detail and isinstance(detail, dict):
                            detail["needs_enrichment"] = False
                            detail["crawl_quality"] = "full"
                            result["detail"] = detail
                            result["status"] = "success"
                        else:
                            result["error"] = "Extraction failed"
                    else:
                        result["error"] = "Selenium crawl failed (None html)"
                else:
                    result["error"] = "No driver available"

            except Exception as e:
                logger.error(f"Error crawling {product_id}: {e}")
                result["error"] = str(e)
            finally:
                if drv:
                    driver_pool.return_driver(drv)

            results.append(result)

    finally:
        driver_pool.close_all()

    # Save to DB - Skip, passing to XCom
    saved_count = 0

    # Minify for XCom
    final_results = []
    success = 0
    for r in results:
        if r.get("status") == "success":
            success += 1

        final_results.append(
            {
                "product_id": r.get("product_id"),
                "url": r.get("url"),
                "status": r.get("status"),
                "error": r.get("error"),
                "detail": r.get("detail"),  # Trả về detail qua XCom
                "crawl_quality": r.get("detail", {}).get("crawl_quality", "full")
                if r.get("detail")
                else "failed",
                "needs_enrichment": False,  # Selenium is the end of line
            }
        )

    logger.info(
        f"✅ SELENIUM BATCH DONE: {success}/{len(product_batch)} success. Saved: {saved_count}"
    )

    return final_results
