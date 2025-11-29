#!/usr/bin/env python
"""Test extract_product_detail with auto-detection of parent category"""

import json
import sys
import os
import importlib.util

# Load hierarchy map directly
print("📖 Loading hierarchy map...")
with open('data/raw/category_hierarchy_map.json', encoding='utf-8') as f:
    hierarchy_map = json.load(f)
print(f"✅ Loaded {len(hierarchy_map)} categories")

# Load the extract_product_detail function
spec = importlib.util.spec_from_file_location(
    "crawl_products_detail",
    "src/pipelines/crawl/crawl_products_detail.py"
)
detail_module = importlib.util.module_from_spec(spec)
try:
    spec.loader.exec_module(detail_module)
    extract_product_detail = detail_module.extract_product_detail
except Exception as e:
    print(f"❌ Error loading crawl_products_detail: {e}")
    sys.exit(1)

# Create mock HTML with 3-level breadcrumb (missing Level 0)
mock_html = """
<html>
<head><title>Test Product</title></head>
<body>
<h1 data-view-id="pdp_product_name">Test Product Name</h1>

<div data-view-id="pdp_breadcrumb">
    <a href="/ngoai-troi-san-vuon/c1884">Ngoài trời & sân vườn</a>
    <a href="/ao-mua-o-du/c12345">Áo mưa, ô dù và phụ kiện đi mưa</a>
    <a href="/phu-kien-di-mua/c12346">Phụ kiện đi mưa</a>
</div>

<div data-view-id="pdp_product_price">
    <span>199,000 VND</span>
</div>

<div class="product-price__list-price">399,000 VND</div>

<div data-view-id="pdp_rating_score">4.5</div>
<div data-view-id="pdp_rating_count">1234</div>
</body>
</html>
"""

# Test extraction
print("\n🔍 Testing extract_product_detail with hierarchy_map...")
result = extract_product_detail(
    mock_html,
    "https://tiki.vn/p/12345678",
    verbose=False,
    hierarchy_map=hierarchy_map
)

print("\n📊 Result:")
print(f"  Product: {result['name']}")
print(f"  Category Path ({len(result['category_path'])} levels):")
for i, level in enumerate(result['category_path']):
    print(f"    {i}. {level}")

# Verify we got 4-5 levels
if len(result['category_path']) == 5:
    print("\n✅ SUCCESS: Category path now has 5 levels (Parent + 4 levels!)")
elif len(result['category_path']) == 4:
    print("\n✅ SUCCESS: Category path now has 4 levels (Level 0 added!)")
elif len(result['category_path']) == 3:
    print("\n⚠️  WARNING: Still only 3 levels")

