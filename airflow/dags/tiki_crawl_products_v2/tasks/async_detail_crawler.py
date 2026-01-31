"""
Async Detail Crawler Integration

This module provides async crawling capabilities for product details.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any


# Feature flag check
def is_async_enabled() -> bool:
    """Check if async crawler is enabled"""
    import os

    try:
        from airflow.models import Variable

        return Variable.get("USE_ASYNC_CRAWLER", default_var="false").lower() in (
            "true",
            "1",
            "yes",
        )
    except Exception:
        return os.getenv("USE_ASYNC_CRAWLER", "false").lower() in ("true", "1", "yes")


async def crawl_product_details_async(
    product_ids: list[str],
    batch_size: int = 50,
    max_concurrent: int = 10,
    logger: logging.Logger | None = None,
) -> list[dict[str, Any]]:
    """
    Crawl product details using async I/O

    Args:
        product_ids: List of product IDs to crawl
        batch_size: Number of products per batch
        max_concurrent: Maximum concurrent requests
        logger: Logger instance

    Returns:
        List of product details
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    try:
        import aiohttp

        from common.async_io import AsyncHTTPClient

        logger.info(f"🚀 Using ASYNC crawler for {len(product_ids)} products")

        # Initialize async HTTP client
        timeout = aiohttp.ClientTimeout(total=15, connect=10)
        client = AsyncHTTPClient(connector_limit=max_concurrent, timeout=timeout)

        results = []

        try:
            # Process in batches
            for i in range(0, len(product_ids), batch_size):
                batch = product_ids[i : i + batch_size]
                logger.info(f"Processing batch {i // batch_size + 1}: {len(batch)} products")

                # Build URLs
                urls = [f"https://tiki.vn/api/v2/products/{pid}" for pid in batch]

                # Fetch concurrently
                responses = await client.get_many(urls)

                # Process responses
                for pid, response in zip(batch, responses, strict=False):
                    if response and not isinstance(response, Exception):
                        try:
                            if hasattr(response, "json"):
                                data = await response.json()
                            else:
                                data = response

                            results.append({"product_id": pid, "status": "success", "data": data})
                        except Exception as e:
                            logger.warning(f"Failed to parse response for {pid}: {e}")
                            results.append({"product_id": pid, "status": "error", "error": str(e)})
                    else:
                        error_msg = (
                            str(response) if isinstance(response, Exception) else "Unknown error"
                        )
                        results.append({"product_id": pid, "status": "error", "error": error_msg})

                # Small delay between batches
                if i + batch_size < len(product_ids):
                    await asyncio.sleep(0.5)

            logger.info(f"✅ Async crawl complete: {len(results)} results")
            return results

        finally:
            await client.close()

    except ImportError as e:
        logger.warning(f"⚠️  Async crawler not available: {e}")
        return []
    except Exception as e:
        logger.error(f"❌ Async crawl failed: {e}")
        return []


def crawl_product_batch_with_async(
    product_batch: list[dict[str, Any]], batch_index: int = 0, logger: logging.Logger | None = None
) -> dict[str, Any]:
    """
    Crawl a batch of products using async if enabled, otherwise use standard method

    Args:
        product_batch: List of products to crawl
        batch_index: Index of this batch
        logger: Logger instance

    Returns:
        Dict with crawl results
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # Check if async is enabled
    if not is_async_enabled():
        logger.info("📝 Async crawler disabled, using standard method")
        return None  # Will use standard crawler

    try:
        # Extract product IDs
        product_ids = [
            p.get("product_id") or p.get("id")
            for p in product_batch
            if p.get("product_id") or p.get("id")
        ]

        if not product_ids:
            logger.warning("No product IDs found in batch")
            return {
                "batch_index": batch_index,
                "status": "error",
                "error": "No product IDs",
                "products": [],
            }

        logger.info(f"🚀 Crawling batch {batch_index} with ASYNC: {len(product_ids)} products")

        # Run async crawl
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            results = loop.run_until_complete(
                crawl_product_details_async(
                    product_ids=product_ids, batch_size=50, max_concurrent=10, logger=logger
                )
            )
        finally:
            loop.close()

        # Process results
        success_count = sum(1 for r in results if r.get("status") == "success")
        error_count = len(results) - success_count

        logger.info(f"✅ Batch {batch_index}: {success_count} success, {error_count} errors")

        # Merge results with original products
        result_map = {r["product_id"]: r for r in results}
        enriched_products = []

        for product in product_batch:
            pid = product.get("product_id") or product.get("id")
            if pid in result_map:
                result = result_map[pid]
                if result.get("status") == "success":
                    # Merge detail data
                    product_copy = product.copy()
                    product_copy.update(result.get("data", {}))
                    product_copy["has_detail"] = True
                    enriched_products.append(product_copy)
                else:
                    # Keep original product
                    product_copy = product.copy()
                    product_copy["has_detail"] = False
                    product_copy["detail_error"] = result.get("error")
                    enriched_products.append(product_copy)
            else:
                enriched_products.append(product)

        return {
            "batch_index": batch_index,
            "status": "success",
            "products": enriched_products,
            "crawled_count": len(product_ids),
            "success_count": success_count,
            "error_count": error_count,
            "method": "async",
        }

    except Exception as e:
        logger.error(f"❌ Async batch crawl failed: {e}")
        return {
            "batch_index": batch_index,
            "status": "error",
            "error": str(e),
            "products": product_batch,
            "method": "async_failed",
        }


# Export
__all__ = [
    "is_async_enabled",
    "crawl_product_details_async",
    "crawl_product_batch_with_async",
]
