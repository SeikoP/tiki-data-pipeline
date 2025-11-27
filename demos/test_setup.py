#!/usr/bin/env python
"""
Simple test script - check if crawl methods work
"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

print("=" * 80)
print("🧪 TESTING CRAWL SETUP")
print("=" * 80)
print()

# Test 1: Check imports
print("📦 Test 1: Checking imports...")
try:
    from pipelines.crawl.crawl_products_detail import (
        extract_product_detail,
        crawl_product_detail_with_selenium,
    )
    print("✅ crawl_products_detail imports OK")
except Exception as e:
    print(f"❌ Error: {e}")
    sys.exit(1)

# Test 2: Check aiohttp
print("\n📦 Test 2: Checking aiohttp...")
try:
    import aiohttp
    print("✅ aiohttp installed")
except ImportError:
    print("⚠️  aiohttp not installed")
    print("   Install: pip install aiohttp")

# Test 3: Check requests
print("\n📦 Test 3: Checking requests...")
try:
    import requests
    print("✅ requests installed")
except ImportError:
    print("⚠️  requests not installed")
    print("   Install: pip install requests")

# Test 4: Check BeautifulSoup
print("\n📦 Test 4: Checking BeautifulSoup...")
try:
    from bs4 import BeautifulSoup
    print("✅ BeautifulSoup installed")
except ImportError:
    print("❌ BeautifulSoup not installed")
    print("   Install: pip install beautifulsoup4")

# Test 5: Test simple HTTP fetch (no Selenium)
print("\n📦 Test 5: Testing simple HTTP fetch...")
try:
    import aiohttp
    import asyncio

    async def test_fetch():
        """Test fetch without Selenium"""
        url = "https://tiki.vn/api/v2/products/1"
        try:
            connector = aiohttp.TCPConnector(ssl=False)
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return True
        except Exception as e:
            print(f"   Error: {e}")
            return False
        return False

    result = asyncio.run(test_fetch())
    if result:
        print("✅ HTTP fetch works (AsyncHTTP method OK)")
    else:
        print("⚠️  HTTP fetch failed")

except ImportError:
    print("⚠️  aiohttp not available")

print("\n" + "=" * 80)
print("✅ SETUP TEST COMPLETED")
print("=" * 80)
print()
print("📝 Next steps:")
print("  1. python demos/demo_crawl_detail_async.py")
print("  2. python demos/demo_crawl_detail_comparison.py")
print()
