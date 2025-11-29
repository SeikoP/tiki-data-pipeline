#!/usr/bin/env python
"""Test auto-detect parent category for 3-4 level paths"""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(0, str(Path(__file__).parent))

from src.pipelines.crawl.config import MAX_CATEGORY_LEVELS
from src.pipelines.crawl.crawl_products_detail import (
    extract_product_detail,
)


def create_test_html(category_breadcrumbs, product_name="Test Product"):
    """Create fake HTML with breadcrumbs"""
    breadcrumb_html = ""
    for name in category_breadcrumbs:
        breadcrumb_html += f'<a href="#">{name}</a>'

    html = f"""
    <html>
    <head><title>{product_name}</title></head>
    <body>
        <h1 data-view-id="pdp_product_name">{product_name}</h1>
        <div data-view-id="pdp_breadcrumb">
            {breadcrumb_html}
        </div>
        <div data-view-id="pdp_product_price">100,000</div>
        <script id="__NEXT_DATA__" type="application/json">
        {{}}
        </script>
    </body>
    </html>
    """
    return html


# Test case 1: 3 level path (missing parent)
print("=" * 80)
print("TEST CASE 1: 3-level path (should add parent)")
print("=" * 80)

html1 = create_test_html(["Trang trí nhà cửa", "Tranh trang trí", "Tranh đá"], "Tranh Phong Cảnh")

result1 = extract_product_detail(html1, "https://tiki.vn/product/123", verbose=False)
print("\nInput breadcrumbs (3 levels): ['Trang trí nhà cửa', 'Tranh trang trí', 'Tranh đá']")
print(f"Result path ({len(result1['category_path'])} levels): {result1['category_path']}")

if len(result1["category_path"]) == 4 and result1["category_path"][0] == "Nhà Cửa - Đời Sống":
    print("✅ PASS: Parent category added correctly!")
elif len(result1["category_path"]) == 4:
    print(
        f"⚠️  WARN: 4 levels but parent is '{result1['category_path'][0]}' (expected 'Nhà Cửa - Đời Sống')"
    )
elif len(result1["category_path"]) == 3:
    print("❌ FAIL: Parent category NOT added (still 3 levels)")
else:
    print(f"❌ FAIL: Unexpected number of levels: {len(result1['category_path'])}")

# Test case 2: 3 level path with different categories
print("\n" + "=" * 80)
print("TEST CASE 2: 3-level path different category (should add parent)")
print("=" * 80)

html2 = create_test_html(
    ["Decal & giấy dán kính", "Tranh dán kính", "Tranh phong cảnh"], "Tranh Dán Tường"
)

result2 = extract_product_detail(html2, "https://tiki.vn/product/456", verbose=False)
print(
    "\nInput breadcrumbs (3 levels): ['Decal & giấy dán kính', 'Tranh dán kính', 'Tranh phong cảnh']"
)
print(f"Result path ({len(result2['category_path'])} levels): {result2['category_path']}")

if len(result2["category_path"]) == 4 and result2["category_path"][0] == "Nhà Cửa - Đời Sống":
    print("✅ PASS: Parent category added correctly!")
elif len(result2["category_path"]) == 4:
    print(
        f"⚠️  WARN: 4 levels but parent is '{result2['category_path'][0]}' (expected 'Nhà Cửa - Đời Sống')"
    )
elif len(result2["category_path"]) == 3:
    print("❌ FAIL: Parent category NOT added (still 3 levels)")
else:
    print(f"❌ FAIL: Unexpected number of levels: {len(result2['category_path'])}")

# Test case 3: 4 level path (should also add parent if missing)
print("\n" + "=" * 80)
print("TEST CASE 3: 4-level path (should add parent if missing)")
print("=" * 80)

html3 = create_test_html(
    ["Trang trí nhà cửa", "Tranh trang trí", "Tranh đá", "Tranh thiên nhiên"], "Tranh Núi"
)

result3 = extract_product_detail(html3, "https://tiki.vn/product/789", verbose=False)
print(
    "\nInput breadcrumbs (4 levels): ['Trang trí nhà cửa', 'Tranh trang trí', 'Tranh đá', 'Tranh thiên nhiên']"
)
print(f"Result path ({len(result3['category_path'])} levels): {result3['category_path']}")

if (
    len(result3["category_path"]) == MAX_CATEGORY_LEVELS
    and result3["category_path"][0] == "Nhà Cửa - Đời Sống"
):
    print("✅ PASS: Parent category added correctly to 4-level path!")
elif len(result3["category_path"]) == MAX_CATEGORY_LEVELS:
    print(
        f"⚠️  WARN: {MAX_CATEGORY_LEVELS} levels but parent is '{result3['category_path'][0]}' (expected 'Nhà Cửa - Đời Sống')"
    )
elif len(result3["category_path"]) == 4:
    print("⚠️  PARTIAL: Still 4 levels, parent not added (but max level check ok)")
else:
    print(f"❌ FAIL: Unexpected number of levels: {len(result3['category_path'])}")

# Summary
print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)
print(
    f"Test 1 (3 levels): {len(result1['category_path'])} levels - {'✅' if len(result1['category_path']) == 4 else '❌'}"
)
print(
    f"Test 2 (3 levels different): {len(result2['category_path'])} levels - {'✅' if len(result2['category_path']) == 4 else '❌'}"
)
print(
    f"Test 3 (4 levels): {len(result3['category_path'])} levels - {'✅' if len(result3['category_path']) == MAX_CATEGORY_LEVELS else '⚠️'}"
)
