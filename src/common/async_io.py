"""
Async I/O layer for performance optimization.

This module provides asynchronous HTTP client functionality using aiohttp
for improved concurrency and throughput in network operations.
"""

import asyncio
import logging
from typing import Any
from urllib.parse import urlparse

import aiohttp

logger = logging.getLogger(__name__)


class AsyncHTTPClient:
    """Async HTTP client with connection pooling and rate limiting.

    This client uses aiohttp for non-blocking HTTP requests, enabling
    high concurrency for network-bound operations.
    """

    def __init__(
        self,
        connector_limit: int = 300,
        timeout_total: int = 15,
        timeout_connect: int = 10,
        rate_limiter: Any | None = None,
    ):
        """Initialize async HTTP client with connection pooling.

        Args:
            connector_limit: Maximum concurrent connections
            timeout_total: Total request timeout in seconds
            timeout_connect: Connection timeout in seconds
            rate_limiter: Optional rate limiter for request throttling
        """
        self.connector_limit = connector_limit
        self.rate_limiter = rate_limiter

        # Create timeout configuration
        self.timeout = aiohttp.ClientTimeout(total=timeout_total, connect=timeout_connect)

        # Session will be created lazily
        self._session: aiohttp.ClientSession | None = None
        self._connector: aiohttp.TCPConnector | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure session is created (lazy initialization)."""
        if self._session is None or self._session.closed:
            self._connector = aiohttp.TCPConnector(
                limit=self.connector_limit,
                limit_per_host=30,  # Reasonable per-host limit
                ttl_dns_cache=300,  # Cache DNS for 5 minutes
            )
            self._session = aiohttp.ClientSession(connector=self._connector, timeout=self.timeout)
        return self._session

    async def get(
        self, url: str, headers: dict[str, str] | None = None, **kwargs
    ) -> aiohttp.ClientResponse | None:
        """Async GET request with rate limiting.

        Args:
            url: URL to fetch
            headers: Optional HTTP headers
            **kwargs: Additional arguments passed to aiohttp.ClientSession.get

        Returns:
            ClientResponse object or None on error
        """
        # Apply rate limiting if configured
        if self.rate_limiter:
            try:
                domain = urlparse(url).netloc or "default"
                await asyncio.to_thread(self.rate_limiter.wait, domain)
            except Exception as e:
                logger.warning(f"Rate limiter error: {e}")

        session = await self._ensure_session()

        try:
            response = await session.get(url, headers=headers, **kwargs)
            return response
        except TimeoutError:
            logger.warning(f"Timeout fetching {url}")
            return None
        except aiohttp.ClientError as e:
            logger.warning(f"Client error fetching {url}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error fetching {url}: {e}")
            return None

    async def get_many(
        self, urls: list[str], headers: dict[str, str] | None = None, **kwargs
    ) -> list[dict[str, Any] | None]:
        """Fetch multiple URLs concurrently with connector limit enforcement.

        Uses a semaphore to ensure concurrent requests don't exceed the
        configured connector limit.

        Args:
            urls: List of URLs to fetch
            headers: Optional HTTP headers
            **kwargs: Additional arguments passed to get()

        Returns:
            List of response data (dicts) in same order as input URLs.
            Failed requests return None.
        """
        # Create semaphore to limit concurrency
        semaphore = asyncio.Semaphore(self.connector_limit)

        async def fetch_with_limit(url):
            async with semaphore:
                return await self._fetch_json(url, headers, **kwargs)

        tasks = [fetch_with_limit(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Convert exceptions to None
        processed_results = []
        for result in results:
            if isinstance(result, Exception):
                logger.warning(f"Request failed: {result}")
                processed_results.append(None)
            else:
                processed_results.append(result)

        return processed_results

    async def _fetch_json(
        self, url: str, headers: dict[str, str] | None = None, **kwargs
    ) -> dict[str, Any] | None:
        """Fetch URL and parse JSON response.

        Args:
            url: URL to fetch
            headers: Optional HTTP headers
            **kwargs: Additional arguments

        Returns:
            Parsed JSON data or None on error
        """
        response = await self.get(url, headers=headers, **kwargs)
        if response is None:
            return None

        try:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                logger.warning(f"Non-200 status {response.status} for {url}")
                return None
        except Exception as e:
            logger.warning(f"Error parsing JSON from {url}: {e}")
            return None
        finally:
            response.close()

    async def close(self) -> None:
        """Close session and cleanup connections."""
        if self._session and not self._session.closed:
            await self._session.close()

        # Wait for connections to close gracefully
        if self._connector:
            await asyncio.sleep(0.25)


async def get_with_retry(
    client: AsyncHTTPClient,
    url: str,
    max_retries: int = 3,
    headers: dict[str, str] | None = None,
    **kwargs,
) -> dict[str, Any] | None:
    """Fetch URL with exponential backoff retry.

    Args:
        client: AsyncHTTPClient instance
        url: URL to fetch
        max_retries: Maximum number of retry attempts
        headers: Optional HTTP headers
        **kwargs: Additional arguments

    Returns:
        Parsed JSON data or None after all retries exhausted
    """
    for attempt in range(max_retries):
        try:
            response = await client.get(url, headers=headers, **kwargs)
            if response is None:
                raise aiohttp.ClientError("No response received")

            if response.status == 200:
                data = await response.json()
                response.close()
                return data
            elif response.status == 429:
                # Rate limited - back off
                response.close()
                raise aiohttp.ClientError("Rate limited (429)")
            else:
                response.close()
                raise aiohttp.ClientError(f"HTTP {response.status}")

        except (TimeoutError, aiohttp.ClientError) as e:
            if attempt == max_retries - 1:
                logger.error(f"Failed to fetch {url} after {max_retries} attempts: {e}")
                return None

            # Exponential backoff: 2^attempt seconds
            wait_time = 2**attempt
            logger.warning(f"Retry {attempt + 1}/{max_retries} for {url} after {wait_time}s: {e}")
            await asyncio.sleep(wait_time)

    return None


class AsyncCrawlerAdapter:
    """Adapter to wrap existing crawler with async I/O capabilities.

    This adapter provides async methods for crawling products using
    the AsyncHTTPClient for improved concurrency.
    """

    def __init__(
        self,
        client: AsyncHTTPClient | None = None,
        batch_size: int = 50,
        connector_limit: int = 300,
    ):
        """Initialize async crawler adapter.

        Args:
            client: Optional AsyncHTTPClient instance (created if not provided)
            batch_size: Number of products to process concurrently
            connector_limit: Maximum concurrent connections
        """
        self.client = client or AsyncHTTPClient(connector_limit=connector_limit)
        self.batch_size = batch_size
        self._owns_client = client is None

    async def crawl_products_async(
        self,
        product_ids: list[str],
        base_url_template: str = "https://tiki.vn/api/v2/products/{product_id}",
        headers: dict[str, str] | None = None,
    ):
        """Crawl products using async I/O (streaming generator).

        Yields products as they are fetched, enabling streaming processing
        without loading all data into memory.

        Args:
            product_ids: List of product IDs to crawl
            base_url_template: URL template with {product_id} placeholder
            headers: Optional HTTP headers

        Yields:
            Product data dictionaries as they are fetched
        """
        # Default headers if not provided
        if headers is None:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*",
            }

        # Process in batches for controlled concurrency
        for i in range(0, len(product_ids), self.batch_size):
            batch = product_ids[i : i + self.batch_size]

            # Build URLs for this batch
            urls = [base_url_template.format(product_id=pid) for pid in batch]

            # Fetch batch concurrently
            results = await self.client.get_many(urls, headers=headers)

            # Yield products as they are processed
            for product_id, result in zip(batch, results, strict=False):
                if result is not None:
                    # Add product_id to result if not present
                    if "id" not in result and "product_id" not in result:
                        result["product_id"] = product_id
                    yield result
                else:
                    # Log failed fetch but continue processing
                    logger.warning(f"Failed to fetch product {product_id}")

    async def crawl_category_products_async(
        self,
        category_id: str,
        max_pages: int = 5,
        items_per_page: int = 48,
        headers: dict[str, str] | None = None,
    ):
        """Crawl products from a category using Tiki listing API.

        Args:
            category_id: Category ID to crawl
            max_pages: Maximum number of pages to fetch
            items_per_page: Number of items per page
            headers: Optional HTTP headers

        Yields:
            Product data dictionaries as they are fetched
        """
        # Default headers
        if headers is None:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json, text/plain, */*",
                "X-Guest-Token": "no-token",
            }

        # Build URLs for all pages
        urls = []
        for page in range(1, max_pages + 1):
            url = (
                f"https://tiki.vn/api/v2/products?"
                f"limit={items_per_page}&"
                f"include=advertisement&"
                f"aggregations=2&"
                f"version=home-perso&"
                f"category={category_id}&"
                f"page={page}"
            )
            urls.append(url)

        # Fetch all pages concurrently
        results = await self.client.get_many(urls, headers=headers)

        # Process results and yield products
        for page_num, result in enumerate(results, start=1):
            if result is None:
                logger.warning(f"Failed to fetch page {page_num} for category {category_id}")
                continue

            products_data = result.get("data", [])
            if not products_data:
                # No more products, stop
                break

            for item in products_data:
                # Extract and normalize product data
                product_id = str(item.get("id", ""))
                if not product_id:
                    continue

                product_url = item.get("url_path", "")
                if product_url and not product_url.startswith("http"):
                    product_url = f"https://tiki.vn/{product_url}"

                # Extract sales count
                sales_count = None
                quantity_sold = item.get("quantity_sold")
                if isinstance(quantity_sold, dict):
                    sales_count = int(quantity_sold.get("value", 0))
                elif quantity_sold is not None:
                    sales_count = quantity_sold

                product = {
                    "product_id": product_id,
                    "name": item.get("name", ""),
                    "url": product_url or f"https://tiki.vn/p/{product_id}.html",
                    "price": item.get("price"),
                    "original_price": item.get("original_price"),
                    "sales_count": sales_count,
                    "image_url": item.get("thumbnail_url", ""),
                    "rating_average": item.get("rating_average"),
                    "review_count": item.get("review_count"),
                    "extraction_method": "async_listing_api",
                }

                yield product

            # Check pagination metadata to see if we should continue
            paging = result.get("paging", {})
            current_page = paging.get("current_page", page_num)
            last_page = paging.get("last_page", 1)

            if current_page >= last_page:
                break

    async def close(self) -> None:
        """Close the HTTP client if owned by this adapter."""
        if self._owns_client:
            await self.client.close()


def chunk_list(items: list[Any], chunk_size: int) -> list[list[Any]]:
    """Split a list into chunks of specified size.

    Args:
        items: List to chunk
        chunk_size: Size of each chunk

    Returns:
        List of chunks
    """
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]
