#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script để test load categories và kiểm tra parent có được include không
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

from pipelines.crawl.storage.postgres_storage import PostgresStorage


def test_load_categories():
    """Test load categories và kiểm tra parent có được include không"""
    
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
    
    print(f"📊 Tìm thấy {len(categories)} categories trong file JSON")
    
    # Build URL -> category map
    url_to_cat_full = {cat.get("url"): cat for cat in categories}
    
    # Tìm category có vấn đề
    problem_cat_url = "https://tiki.vn/vat-pham-phong-thuy/c5848"
    if problem_cat_url not in url_to_cat_full:
        print(f"❌ Không tìm thấy category {problem_cat_url} trong file JSON")
        return
    
    problem_cat = url_to_cat_full[problem_cat_url]
    print(f"\n📌 Category có vấn đề:")
    print(f"   Name: {problem_cat.get('name')}")
    print(f"   URL: {problem_cat.get('url')}")
    print(f"   Parent URL: {problem_cat.get('parent_url')}")
    
    # Traverse parent chain
    parent_urls_needed = set()
    current = problem_cat
    visited = set()
    depth = 0
    chain = []
    
    print(f"\n🔗 Parent Chain:")
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
    
    print(f"\n📊 Tổng kết:")
    print(f"   - Số parent URLs cần thiết: {len(parent_urls_needed)}")
    print(f"   - Parent URLs: {list(parent_urls_needed)}")
    
    # Kiểm tra từng parent có trong file JSON không
    print(f"\n🔍 Kiểm tra từng parent:")
    for parent_url in parent_urls_needed:
        if parent_url in url_to_cat_full:
            parent_cat = url_to_cat_full[parent_url]
            print(f"   ✅ {parent_url}: {parent_cat.get('name')}")
        else:
            print(f"   ❌ {parent_url}: KHÔNG CÓ trong file JSON")
    
    # Test với storage
    print(f"\n🧪 Test với PostgresStorage:")
    storage = PostgresStorage()
    
    # Simulate logic trong load_categories_to_db.py
    used_category_ids = set()
    try:
        used_category_ids = storage.get_used_category_ids()
        print(f"   Found {len(used_category_ids)} active categories in products table")
    except Exception as e:
        print(f"   ⚠️  Could not get used category IDs: {e}")
    
    # Check if problem category has products
    problem_cat_id = problem_cat.get("category_id")
    if not problem_cat_id:
        import re
        match = re.search(r"c?(\d+)", problem_cat.get("url", ""))
        if match:
            problem_cat_id = f"c{match.group(1)}"
    
    if problem_cat_id in used_category_ids:
        print(f"   ✅ Category {problem_cat_id} có products")
    else:
        print(f"   ⚠️  Category {problem_cat_id} KHÔNG có products")
    
    storage.close()


if __name__ == "__main__":
    test_load_categories()
