
import asyncio
import aiohttp
import sys
import os

# Add src to path
sys.path.append("/opt/airflow/src")

from pipelines.crawl.crawl_products_detail import crawl_product_detail_async

async def test_crawl():
    product_id = "279153782"
    url = f"https://tiki.vn/p{product_id}.html"
    
    print(f"Testing crawl for URL: {url}")
    print("-" * 50)
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
    }
    
    async with aiohttp.ClientSession(headers=headers) as session:
        # Gọi hàm với verbose=True
        result = await crawl_product_detail_async(
            url=url, 
            session=session, 
            use_selenium_fallback=False, 
            verbose=True
        )
        
        print("-" * 50)
        if result:
            print("✅ CRAWL SUCCESS!")
            print(f"Name: {result.get('name')}")
            print(f"Price: {result.get('price')}")
        else:
            print("❌ CRAWL FAILED (Result is None)")

if __name__ == "__main__":
    asyncio.run(test_crawl())
