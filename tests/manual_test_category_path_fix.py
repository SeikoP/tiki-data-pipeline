#!/usr/bin/env python
"""
Test that product names are NOT included in category_path.
"""

import os
import sys

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))


from pipelines.crawl.crawl_products_detail import (
    crawl_product_detail_with_selenium,
    extract_product_detail,
)

# Test with a few real Tiki products
test_urls = [
    "https://tiki.vn/p/276665412",  # Nạo rau
    "https://tiki.vn/p/100000001",  # Some random product
]

print("=" * 80)
print("TESTING: Product names should NOT appear in category_path")
print("=" * 80)

for url in test_urls:
    try:
        print(f"\n🔍 Testing: {url}")

        # Get HTML first
        html = crawl_product_detail_with_selenium(url, verbose=False)
        if not html:
            print("   ⚠️  Failed to get HTML")
            continue

        # Extract product detail from HTML
        product = extract_product_detail(html, url, verbose=False)

        if product:
            name = product.get("name", "UNKNOWN")
            category_path = product.get("category_path", [])

            print(f"   Name: {name[:60]}...")
            print(f"   Category Path: {' > '.join(category_path)}")

            # Check if product name is in category_path
            if name in category_path:
                print("   ❌ ERROR: Product name found in category_path!")
            else:
                print("   ✅ OK: Product name NOT in category_path")

            # Verify it looks like valid categories
            if len(category_path) > 0 and all(
                isinstance(c, str) and len(c) > 0 for c in category_path
            ):
                print(f"   ✅ Category path looks valid ({len(category_path)} levels)")
            else:
                print("   ⚠️  Category path seems empty or invalid")
        else:
            print("   ⚠️  Failed to extract product data")

    except Exception as e:
        print(f"   ⚠️  Error: {e}")

print("\n" + "=" * 80)
print("TEST COMPLETE")
print("=" * 80)
