#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script để test logic load categories và verify parent categories được include đầy đủ
"""

import os
import sys
import json
from pathlib import Path

# Fix encoding cho Windows
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

# Thêm src vào path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

# Set environment variables
if "POSTGRES_HOST" not in os.environ or os.environ["POSTGRES_HOST"] == "postgres":
    os.environ["POSTGRES_HOST"] = "localhost"


def test_load_logic():
    """Test logic load categories"""
    
    # Tìm file JSON
    json_files = [
        project_root / "data" / "raw" / "categories_recursive_optimized.json",
        project_root / "data" / "raw" / "categories_recursive.json",
        project_root / "data" / "raw" / "categories.json",
    ]
    
    json_file = None
    for f in json_files:
        if f.exists():
            json_file = f
            break
    
    if not json_file:
        print("❌ Không tìm thấy file categories JSON")
        return
    
    print(f"📂 Đang đọc file: {json_file}")
    with open(json_file, encoding="utf-8") as f:
        categories = json.load(f)
    
    print(f"📊 Loaded {len(categories)} categories từ file JSON")
    
    # Simulate logic trong load_categories_to_db.py
    url_to_cat_full = {cat.get("url"): cat for cat in categories}
    
    # Test với category có vấn đề
    test_cat_url = "https://tiki.vn/vat-pham-phong-thuy/c5848"
    if test_cat_url not in url_to_cat_full:
        print(f"❌ Không tìm thấy test category {test_cat_url}")
        return
    
    test_cat = url_to_cat_full[test_cat_url]
    print(f"\n📌 Test Category: {test_cat.get('name')}")
    print(f"   URL: {test_cat.get('url')}")
    print(f"   Parent URL: {test_cat.get('parent_url')}")
    
    # Check if leaf
    parent_urls_in_list = {c.get("parent_url") for c in categories if c.get("parent_url")}
    is_leaf = test_cat.get("url") not in parent_urls_in_list
    print(f"   Is Leaf: {is_leaf}")
    
    # Traverse parent chain
    print(f"\n🔗 Parent Chain:")
    parent_urls_needed = set()
    current = test_cat
    visited = set()
    depth = 0
    chain = []
    
    while current and depth < 10:
        chain.append(current)
        print(f"   {depth + 1}. [{current.get('level', '?')}] {current.get('name')} ({current.get('url')})")
        
        parent_url = current.get("parent_url")
        if not parent_url:
            break
        if parent_url in visited:
            break
        visited.add(parent_url)
        parent_urls_needed.add(parent_url)
        
        if parent_url in url_to_cat_full:
            current = url_to_cat_full[parent_url]
        else:
            print(f"   ⚠️  Parent {parent_url} KHÔNG có trong file JSON!")
            break
        depth += 1
    
    print(f"\n📊 Kết quả:")
    print(f"   - Số parent URLs cần thiết: {len(parent_urls_needed)}")
    print(f"   - Parent URLs: {list(parent_urls_needed)}")
    
    # Verify tất cả parents có trong file JSON
    print(f"\n✅ Verification:")
    all_found = True
    for parent_url in parent_urls_needed:
        if parent_url in url_to_cat_full:
            parent_cat = url_to_cat_full[parent_url]
            print(f"   ✅ {parent_url}: {parent_cat.get('name')}")
        else:
            print(f"   ❌ {parent_url}: KHÔNG CÓ trong file JSON")
            all_found = False
    
    if all_found:
        print(f"\n✅ Tất cả parent categories đều có trong file JSON!")
        print(f"   Logic load sẽ hoạt động đúng.")
    else:
        print(f"\n❌ Một số parent categories không có trong file JSON!")
        print(f"   Cần kiểm tra lại file JSON hoặc logic load.")


if __name__ == "__main__":
    test_load_logic()
