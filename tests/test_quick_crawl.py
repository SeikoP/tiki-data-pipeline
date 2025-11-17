"""
Quick test: Crawl 5 products để test performance
"""

import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from common.monitoring import PerformanceMetrics, PerformanceTimer  # noqa: E402

print("=" * 70)
print("🚀 QUICK PERFORMANCE TEST - Crawl 5 Products")
print("=" * 70)
print()

# Use a known category with products (Điện Thoại - Máy Tính Bảng)
test_category = {
    "name": "Điện Thoại - Máy Tính Bảng",
    "url": "https://tiki.vn/dien-thoai-may-tinh-bang/c1789",
    "product_count": 10000,  # Known to have many products
}

print(f"📁 Test category: {test_category['name']}")
print(f"   URL: {test_category['url']}")
print(f"   Products: {test_category.get('product_count', 'unknown')}")
print()

# Test with monitoring
metrics = PerformanceMetrics()

print("⏱️  Starting crawl test (limit: 5 products)...")
print()

import requests  # noqa: E402

import pipelines.crawl.utils as crawl_utils_module  # noqa: E402

# Get functions from the actual utils.py file (not __init__.py)
setup_utf8_encoding = getattr(crawl_utils_module, "setup_utf8_encoding", lambda: None)


def parse_sales_count(text):
    """Fallback parse_sales_count"""
    if not text:
        return 0
    import re

    match = re.search(r"(\d+)", text.replace(".", "").replace(",", ""))
    return int(match.group(1)) if match else 0


def parse_price(text):
    """Fallback parse_price"""
    if not text:
        return 0
    import re

    match = re.search(r"(\d+)", text.replace(".", "").replace(",", ""))
    return int(match.group(1)) if match else 0


def normalize_url(url):
    """Fallback normalize_url"""
    if not url:
        return ""
    if url.startswith("http"):
        return url
    return f"https://tiki.vn{url}"


setup_utf8_encoding()


# Crawl function with timing
def quick_crawl_test(category_url, limit=5):
    """Quick crawl test với monitoring"""

    with metrics.timer("total_crawl"):
        products = []

        # Parse category URL to get category_id
        from urllib.parse import parse_qs, urlparse

        parsed = urlparse(category_url)
        query_params = parse_qs(parsed.query)

        category_id = query_params.get("category", [None])[0]
        if not category_id:
            # Try to extract from path (e.g. /c12345)
            import re

            match = re.search(r"/c(\d+)", parsed.path)
            if match:
                category_id = match.group(1)

        if not category_id:
            print("❌ Could not extract category_id")
            return []

        # API URL
        api_url = "https://tiki.vn/api/v2/products"

        params = {"limit": limit, "category": category_id, "page": 1}

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
        }

        with metrics.timer("api_request"):
            try:
                response = requests.get(api_url, params=params, headers=headers, timeout=30)
                response.raise_for_status()
                data = response.json()
            except Exception as e:
                print(f"❌ API error: {e}")
                return []

        items = data.get("data", [])

        print(f"✅ Fetched {len(items)} products from API")

        for item in items:
            with metrics.timer("process_product"):
                product = {
                    "id": item.get("id"),
                    "name": item.get("name", ""),
                    "url": normalize_url(item.get("url_path", "")),
                    "price": item.get("price"),
                    "list_price": item.get("list_price"),
                    "discount": item.get("discount"),
                    "discount_rate": item.get("discount_rate"),
                    "rating_average": item.get("rating_average"),
                    "review_count": item.get("review_count"),
                    "thumbnail_url": item.get("thumbnail_url"),
                    "seller_name": item.get("current_seller", {}).get("name"),
                }

                # Parse sales count
                qty_sold = item.get("quantity_sold", {})
                if isinstance(qty_sold, dict):
                    sales_text = qty_sold.get("text", "")
                    product["sales_count"] = parse_sales_count(sales_text)

                products.append(product)

        return products


# Run the test
with PerformanceTimer("complete_test", verbose=True):
    results = quick_crawl_test(test_category["url"], limit=5)

print()
print("=" * 70)
print("📊 PERFORMANCE RESULTS")
print("=" * 70)
print()

if results:
    print(f"✅ Successfully crawled {len(results)} products")
    print()

    # Show timing breakdown
    print("⏱️  Timing breakdown:")

    for operation in ["api_request", "process_product", "total_crawl"]:
        stats = metrics.get_stats(operation)
        if stats:
            print(f"   {operation}:")
            print(f"      Total time: {stats['total']:.3f}s")
            print(f"      Count: {stats['count']}")
            if stats["count"] > 1:
                print(f"      Average: {stats['mean']:.3f}s")
                print(f"      Min/Max: {stats['min']:.3f}s / {stats['max']:.3f}s")

    print()
    print("🎯 Performance Analysis:")

    total_stats = metrics.get_stats("total_crawl")
    if total_stats:
        total_time = total_stats["total"]
        per_product = total_time / len(results) if results else 0

        print(f"   • Total time: {total_time:.2f}s")
        print(f"   • Time per product: {per_product:.2f}s")
        print()
        print("   Expected improvements from optimizations:")
        print("   • Selenium wait: 10s → 3s (70% reduction)")
        print("   • Sleep times: 2s → 0.5s (75% reduction)")
        print("   • Redis pooling: 20-30% faster")
        print()

        # Estimate for full crawl
        est_products = 1000
        est_time_minutes = (per_product * est_products) / 60
        print(f"   Estimated time for {est_products} products: {est_time_minutes:.1f} minutes")
else:
    print("❌ No products crawled")

print()
