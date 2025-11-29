#!/usr/bin/env python
"""
Build category hierarchy map to determine parent categories for products.
This helps fix the missing Level 0 issue.

Usage:
    python build_category_hierarchy.py

Output:
    data/raw/category_hierarchy_map.json
    {
        "https://tiki.vn/nha-cua-doi-song/c1883": {
            "name": "Nhà cửa - đời sống",
            "parent_url": None,
            "level": 0
        },
        "https://tiki.vn/ngoai-troi-san-vuon/c1884": {
            "name": "Ngoài trời & sân vườn",
            "parent_url": "https://tiki.vn/nha-cua-doi-song/c1883",
            "level": 1
        }
    }
"""

import json
import os
from collections import defaultdict


def normalize_category_name(name):
    """Normalize category names to consistent format (Title Case)"""
    # Map of common variations to standard format
    name_map = {
        "Nhà cửa - đời sống": "Nhà Cửa - Đời Sống",
        "nhà cửa - đời sống": "Nhà Cửa - Đời Sống",
    }

    return name_map.get(name, name)


def build_category_hierarchy():
    """Build category hierarchy from crawled categories"""

    categories_file = "data/raw/categories_recursive_optimized.json"
    if not os.path.exists(categories_file):
        categories_file = "data/raw/categories_recursive.json"
    if not os.path.exists(categories_file):
        categories_file = "data/raw/categories.json"

    if not os.path.exists(categories_file):
        print(f"❌ File not found: {categories_file}")
        return {}

    with open(categories_file, encoding="utf-8") as f:
        categories = json.load(f)

    print(f"📖 Loaded {len(categories)} categories from {categories_file}")

    # Build mapping: URL → category info
    url_to_category = {}
    children_by_parent = defaultdict(list)

    for cat in categories:
        url = cat["url"]
        parent_url = cat.get("parent_url")
        level = cat.get("level", 0)
        name = normalize_category_name(cat["name"])

        url_to_category[url] = {
            "name": name,
            "parent_url": parent_url,
            "level": level,
            "slug": cat.get("slug", ""),
        }

        if parent_url:
            children_by_parent[parent_url].append(url)

    # Add root category (Level 0)
    root_url = "https://tiki.vn/nha-cua-doi-song/c1883"
    if root_url not in url_to_category:
        url_to_category[root_url] = {
            "name": "Nhà Cửa - Đời Sống",
            "parent_url": None,
            "level": 0,
            "slug": "nha-cua-doi-song",
        }

    # Function to get all parent URLs for a category
    def get_parent_chain(url):
        """Get list of parent URLs from root to this category"""
        chain = []
        current = url_to_category.get(url)
        if not current:
            return chain

        visited = set()
        while current and current.get("parent_url") and current["parent_url"] not in visited:
            parent_url = current["parent_url"]
            chain.insert(0, parent_url)
            visited.add(parent_url)
            current = url_to_category.get(parent_url)

        return chain

    # Build full path for each category
    for url in url_to_category:
        chain = get_parent_chain(url)
        url_to_category[url]["parent_chain"] = chain
        url_to_category[url]["level_actual"] = len(chain)

    # Save mapping
    output_file = "data/raw/category_hierarchy_map.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(url_to_category, f, ensure_ascii=False, indent=2)

    print(f"✅ Saved hierarchy map to: {output_file}")

    # Statistics
    level_counts = defaultdict(int)
    for cat_info in url_to_category.values():
        level = cat_info.get("level_actual", 0)
        level_counts[level] += 1

    print("\n📊 Category hierarchy statistics:")
    for level in sorted(level_counts.keys()):
        print(f"  Level {level}: {level_counts[level]} categories")

    return url_to_category


def get_parent_category_for_product(category_url):
    """Get immediate parent category name for a product"""

    hierarchy_file = "data/raw/category_hierarchy_map.json"
    if not os.path.exists(hierarchy_file):
        print("⚠️  Hierarchy map not found. Run build_category_hierarchy() first.")
        return None

    with open(hierarchy_file, encoding="utf-8") as f:
        hierarchy = json.load(f)

    cat_info = hierarchy.get(category_url)
    if not cat_info:
        return None

    parent_chain = cat_info.get("parent_chain", [])
    if parent_chain:
        parent_url = parent_chain[0]  # Immediate parent
        parent_info = hierarchy.get(parent_url)
        if parent_info:
            return parent_info["name"]

    return None


if __name__ == "__main__":
    hierarchy = build_category_hierarchy()

    # Example: Get root category info
    root_url = "https://tiki.vn/nha-cua-doi-song/c1883"
    if root_url in hierarchy:
        print("\n🏠 Root category:")
        print(f"  Name: {hierarchy[root_url]['name']}")
        print(f"  Level: {hierarchy[root_url]['level_actual']}")
        children_count = len([v for v in hierarchy.values() if v.get("parent_url") == root_url])
        print(f"  Children: {children_count}")
