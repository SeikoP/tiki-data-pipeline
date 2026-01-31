from __future__ import annotations

from tiki_crawl_products_v2.bootstrap import (  # crawl_product_detail_async,  <-- REMOVED
    Any,
    crawl_product_detail_with_driver,
    datetime,
    get_hierarchy_map,
    get_logger,
    get_variable,
)
from tiki_crawl_products_v2.tasks.common import (
    get_selenium_driver_pool_class,
)


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

    rate_limit_delay = float(get_variable("TIKI_DETAIL_RATE_LIMIT_DELAY", default="0.05"))
    max_concurrent = int(get_variable("TIKI_DETAIL_MAX_CONCURRENT_TASKS", default="5"))
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
                    result["status"] = "success"

                    # Logic: Needs Enrichment?
                    # Check for critical fields
                    has_brand = detail.get("brand") and detail.get("brand") != "Unknown"
                    has_specs = bool(detail.get("specifications"))
                    has_seller = bool(detail.get("seller", {}).get("name"))

                    if not has_brand or not has_specs or not has_seller:
                        result["detail"]["needs_enrichment"] = True
                        result["detail"]["crawl_quality"] = "partial"
                    else:
                        result["detail"]["needs_enrichment"] = False
                        result["detail"]["crawl_quality"] = "full"
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
) -> list[dict[str, Any]]:
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
        return []

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
    needs_enrich = 0

    for r in results:
        status = r.get("status")
        detail = r.get("detail") or {}
        is_success = status == "success"

        if is_success:
            success += 1
            if detail.get("needs_enrichment"):
                needs_enrich += 1

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
        f"✅ FAST BATCH DONE: {success}/{len(product_batch)} success. Needs Enrichment: {needs_enrich}. Saved: {saved_count}"
    )
    return final_results


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
    pool_size = int(get_variable("TIKI_DETAIL_POOL_SIZE", default="2"))
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
