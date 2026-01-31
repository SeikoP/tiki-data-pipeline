"""Example usage of the async I/O layer for performance optimization.

This example demonstrates how to use AsyncHTTPClient and AsyncCrawlerAdapter
for high-performance concurrent HTTP requests.
"""

import asyncio
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.async_io import AsyncHTTPClient, AsyncCrawlerAdapter, get_with_retry


async def example_basic_async_client():
    """Example 1: Basic async HTTP client usage."""
    print("\n=== Example 1: Basic Async HTTP Client ===")
    
    # Create client with connection pooling
    client = AsyncHTTPClient(
        connector_limit=50,  # Max 50 concurrent connections
        timeout_total=15,
        timeout_connect=10
    )
    
    # Fetch multiple URLs concurrently
    urls = [
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
        "https://jsonplaceholder.typicode.com/posts/3",
    ]
    
    print(f"Fetching {len(urls)} URLs concurrently...")
    results = await client.get_many(urls)
    
    for i, result in enumerate(results, 1):
        if result:
            print(f"  ✅ URL {i}: Got post with title: {result.get('title', 'N/A')[:50]}...")
        else:
            print(f"  ❌ URL {i}: Failed to fetch")
    
    await client.close()


async def example_retry_logic():
    """Example 2: Retry logic with exponential backoff."""
    print("\n=== Example 2: Retry Logic ===")
    
    client = AsyncHTTPClient(connector_limit=10)
    
    # This URL will likely fail, demonstrating retry logic
    url = "https://httpstat.us/500"  # Returns 500 error
    
    print(f"Fetching {url} with retry logic...")
    result = await get_with_retry(
        client,
        url,
        max_retries=3
    )
    
    if result:
        print(f"  ✅ Success: {result}")
    else:
        print(f"  ❌ Failed after retries (expected for 500 error)")
    
    await client.close()


async def example_crawler_adapter():
    """Example 3: AsyncCrawlerAdapter for product crawling."""
    print("\n=== Example 3: Async Crawler Adapter ===")
    
    # Create adapter
    adapter = AsyncCrawlerAdapter(
        batch_size=10,
        connector_limit=50
    )
    
    # Simulate crawling products (using test API)
    product_ids = [str(i) for i in range(1, 6)]
    
    print(f"Crawling {len(product_ids)} products asynchronously...")
    
    count = 0
    async for product in adapter.crawl_products_async(
        product_ids,
        base_url_template="https://jsonplaceholder.typicode.com/posts/{product_id}"
    ):
        count += 1
        print(f"  ✅ Product {count}: {product.get('title', 'N/A')[:50]}...")
    
    print(f"Total products crawled: {count}")
    
    await adapter.close()


async def example_category_crawling():
    """Example 4: Category product crawling (simulated)."""
    print("\n=== Example 4: Category Product Crawling ===")
    
    adapter = AsyncCrawlerAdapter(
        batch_size=20,
        connector_limit=100
    )
    
    # Note: This would work with real Tiki API
    # For demo, we'll just show the structure
    print("Structure for crawling category products:")
    print("""
    async for product in adapter.crawl_category_products_async(
        category_id="1882",
        max_pages=5,
        items_per_page=48
    ):
        # Process each product as it's fetched
        print(f"Product: {product['name']}")
    """)
    
    await adapter.close()


async def main():
    """Run all examples."""
    print("=" * 60)
    print("Async I/O Layer Examples")
    print("=" * 60)
    
    await example_basic_async_client()
    await example_retry_logic()
    await example_crawler_adapter()
    await example_category_crawling()
    
    print("\n" + "=" * 60)
    print("All examples completed!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
