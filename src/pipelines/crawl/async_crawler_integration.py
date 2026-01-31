"""
Async crawler integration for performance optimization.

This module provides integration between the async I/O layer and the existing
synchronous crawler, with feature flags for gradual rollout.
"""

import asyncio
import logging
from typing import Any

from common.async_io import AsyncCrawlerAdapter, AsyncHTTPClient
from common.config import OPTIMIZATION_CONFIG

logger = logging.getLogger(__name__)


class AsyncCrawlerIntegration:
    """Integration layer for async crawler with backward compatibility.

    This class provides a unified interface that can use either async or
    synchronous crawling based on feature flags, maintaining backward
    compatibility with existing code.
    """

    def __init__(
        self,
        use_async: bool | None = None,
        rate_limiter: Any | None = None,
        connector_limit: int | None = None,
        batch_size: int | None = None,
        timeout_total: int | None = None,
        timeout_connect: int | None = None,
    ):
        """Initialize async crawler integration.

        Args:
            use_async: Enable async mode (defaults to config)
            rate_limiter: Optional rate limiter instance
            connector_limit: Max concurrent connections (defaults to config)
            batch_size: Batch size for concurrent requests (defaults to config)
            timeout_total: Total timeout in seconds (defaults to config)
            timeout_connect: Connection timeout in seconds (defaults to config)
        """
        # Use config values as defaults
        self.use_async = (
            use_async if use_async is not None else OPTIMIZATION_CONFIG["use_async_crawler"]
        )
        self.connector_limit = connector_limit or OPTIMIZATION_CONFIG["async_connector_limit"]
        self.batch_size = batch_size or OPTIMIZATION_CONFIG["async_batch_size"]
        self.timeout_total = timeout_total or OPTIMIZATION_CONFIG["async_timeout_total"]
        self.timeout_connect = timeout_connect or OPTIMIZATION_CONFIG["async_timeout_connect"]

        # Initialize async components if enabled
        self._async_client: AsyncHTTPClient | None = None
        self._async_adapter: AsyncCrawlerAdapter | None = None
        self._rate_limiter = rate_limiter

        if self.use_async:
            self._initialize_async_components()
            logger.info(
                f"✅ Async crawler enabled (connector_limit={self.connector_limit}, batch_size={self.batch_size})"
            )
        else:
            logger.info("ℹ️  Async crawler disabled, using synchronous mode")

    def _initialize_async_components(self):
        """Initialize async HTTP client and crawler adapter."""
        self._async_client = AsyncHTTPClient(
            connector_limit=self.connector_limit,
            timeout_total=self.timeout_total,
            timeout_connect=self.timeout_connect,
            rate_limiter=self._rate_limiter,
        )

        self._async_adapter = AsyncCrawlerAdapter(
            client=self._async_client,
            batch_size=self.batch_size,
            connector_limit=self.connector_limit,
        )

    def crawl_category_products(
        self,
        category_id: str,
        max_pages: int = 5,
        items_per_page: int = 48,
        headers: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Crawl products from a category (sync interface).

        This method provides a synchronous interface that internally uses
        async I/O if enabled, maintaining backward compatibility.

        Args:
            category_id: Category ID to crawl
            max_pages: Maximum number of pages
            items_per_page: Items per page
            headers: Optional HTTP headers

        Returns:
            List of product dictionaries
        """
        if self.use_async and self._async_adapter:
            # Use async implementation
            return asyncio.run(
                self._crawl_category_async(category_id, max_pages, items_per_page, headers)
            )
        else:
            # Fallback to synchronous implementation
            logger.warning("Async mode disabled, falling back to synchronous crawler")
            return self._crawl_category_sync(category_id, max_pages, items_per_page, headers)

    async def _crawl_category_async(
        self, category_id: str, max_pages: int, items_per_page: int, headers: dict[str, str] | None
    ) -> list[dict[str, Any]]:
        """Async implementation of category crawling."""
        products = []

        async for product in self._async_adapter.crawl_category_products_async(
            category_id=category_id,
            max_pages=max_pages,
            items_per_page=items_per_page,
            headers=headers,
        ):
            products.append(product)

        return products

    def _crawl_category_sync(
        self, category_id: str, max_pages: int, items_per_page: int, headers: dict[str, str] | None
    ) -> list[dict[str, Any]]:
        """Synchronous fallback implementation.

        This imports and uses the existing synchronous crawler to maintain
        backward compatibility when async mode is disabled.
        """
        try:
            from .crawl_products import get_products_via_listing_api

            # Build a category URL for the existing function
            category_url = f"https://tiki.vn/c{category_id}"

            products = get_products_via_listing_api(
                category_url=category_url, max_pages=max_pages, use_rate_limiting=True
            )

            return products or []

        except Exception as e:
            logger.error(f"Synchronous crawler fallback failed: {e}")
            return []

    def crawl_products_by_ids(
        self,
        product_ids: list[str],
        base_url_template: str = "https://tiki.vn/api/v2/products/{product_id}",
        headers: dict[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        """Crawl products by IDs (sync interface).

        Args:
            product_ids: List of product IDs
            base_url_template: URL template with {product_id} placeholder
            headers: Optional HTTP headers

        Returns:
            List of product dictionaries
        """
        if self.use_async and self._async_adapter:
            # Use async implementation
            return asyncio.run(
                self._crawl_products_by_ids_async(product_ids, base_url_template, headers)
            )
        else:
            # Fallback to synchronous implementation
            logger.warning("Async mode disabled, falling back to synchronous crawler")
            return self._crawl_products_by_ids_sync(product_ids, base_url_template, headers)

    async def _crawl_products_by_ids_async(
        self, product_ids: list[str], base_url_template: str, headers: dict[str, str] | None
    ) -> list[dict[str, Any]]:
        """Async implementation of product ID crawling."""
        products = []

        async for product in self._async_adapter.crawl_products_async(
            product_ids=product_ids, base_url_template=base_url_template, headers=headers
        ):
            products.append(product)

        return products

    def _crawl_products_by_ids_sync(
        self, product_ids: list[str], base_url_template: str, headers: dict[str, str] | None
    ) -> list[dict[str, Any]]:
        """Synchronous fallback for product ID crawling."""
        import requests

        products = []
        headers = headers or {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
        }

        for product_id in product_ids:
            try:
                url = base_url_template.format(product_id=product_id)
                response = requests.get(url, headers=headers, timeout=15)

                if response.status_code == 200:
                    data = response.json()
                    if "id" not in data and "product_id" not in data:
                        data["product_id"] = product_id
                    products.append(data)
                else:
                    logger.warning(
                        f"Failed to fetch product {product_id}: HTTP {response.status_code}"
                    )

            except Exception as e:
                logger.warning(f"Error fetching product {product_id}: {e}")
                continue

        return products

    def close(self):
        """Close async resources if initialized."""
        if self._async_adapter:
            asyncio.run(self._async_adapter.close())
            logger.info("✅ Async crawler resources closed")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()


def get_async_crawler_integration(
    use_async: bool | None = None, rate_limiter: Any | None = None, **kwargs
) -> AsyncCrawlerIntegration:
    """Factory function to create AsyncCrawlerIntegration instance.

    Args:
        use_async: Enable async mode (defaults to config)
        rate_limiter: Optional rate limiter instance
        **kwargs: Additional arguments passed to AsyncCrawlerIntegration

    Returns:
        AsyncCrawlerIntegration instance
    """
    return AsyncCrawlerIntegration(use_async=use_async, rate_limiter=rate_limiter, **kwargs)
