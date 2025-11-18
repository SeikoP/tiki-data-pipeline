#!/usr/bin/env python3
"""
Script để enrich file categories với category_id và category_path

Công việc:
1. Đọc file categories_recursive_optimized.json
2. Thêm category_id cho mỗi category (extract từ URL)
3. Xây dựng category_path (đường dẫn phân cấp) dựa trên parent_url
4. Ghi lại file categories đã enriched
"""

import json
import re
from pathlib import Path
from typing import Dict, List, Optional


def extract_category_id_from_url(url: Optional[str]) -> Optional[str]:
    """Extract category_id từ URL, e.g., /c1883 -> c1883"""
    if not url:
        return None
    match = re.search(r"/c(\d+)", url)
    return f"c{match.group(1)}" if match else None


def enrich_categories_with_paths(categories_file: Path) -> None:
    """
    Enrich categories với category_id và category_path
    
    Args:
        categories_file: Path to categories JSON file
    """
    print("=" * 70)
    print("🔧 ENRICH CATEGORIES WITH PATHS")
    print("=" * 70)
    
    if not categories_file.exists():
        print(f"❌ File không tồn tại: {categories_file}")
        return
    
    # Step 1: Đọc categories từ file
    print(f"\n📖 Đang đọc file: {categories_file}")
    with open(categories_file, encoding="utf-8") as f:
        categories = json.load(f)
    print(f"✅ Đã đọc {len(categories)} categories")
    
    # Step 2: Build lookup map URL -> category (với category_id và name)
    url_to_category: Dict[str, Dict] = {}
    for cat in categories:
        cat_id = extract_category_id_from_url(cat.get("url"))
        if cat_id and not cat.get("category_id"):
            cat["category_id"] = cat_id
        
        if cat.get("url"):
            url_to_category[cat["url"]] = {
                "name": cat.get("name"),
                "category_id": cat.get("category_id"),
                "parent_url": cat.get("parent_url"),
                "level": cat.get("level", 0),
            }
    
    print(f"✅ Built lookup map với {len(url_to_category)} URLs")
    
    # Step 3: Build category_path cho mỗi category
    def build_path(cat_url: str, depth: int = 0) -> List[str]:
        """
        Recursively build category path (breadcrumb) từ child -> root
        
        Returns list of category names từ root -> child
        """
        if depth > 20:  # Prevent infinite loops
            return []
        
        if cat_url not in url_to_category:
            return []
        
        cat_info = url_to_category[cat_url]
        current_name = cat_info.get("name")
        parent_url = cat_info.get("parent_url")
        
        if not current_name:
            return []
        
        # If có parent, recursively build path
        if parent_url and parent_url != cat_url:
            parent_path = build_path(parent_url, depth + 1)
            return parent_path + [current_name]
        else:
            # Root category
            return [current_name]
    
    # Step 4: Add category_path to each category
    enriched_count = 0
    for cat in categories:
        cat_url = cat.get("url")
        if cat_url and not cat.get("category_path"):
            path = build_path(cat_url)
            if path:
                cat["category_path"] = path
                enriched_count += 1
    
    print(f"✅ Enriched category_path cho {enriched_count} categories")
    
    # Step 5: Statistics
    print("\n📊 Thống kê categories:")
    print(f"   - Tổng cộng: {len(categories)}")
    
    # Count by level
    level_counts = {}
    for cat in categories:
        level = cat.get("level", 0)
        level_counts[level] = level_counts.get(level, 0) + 1
    
    print("   - Theo level:")
    for level in sorted(level_counts.keys()):
        print(f"     - Level {level}: {level_counts[level]}")
    
    # Count with category_id
    with_id = sum(1 for cat in categories if cat.get("category_id"))
    print(f"   - Có category_id: {with_id}")
    
    # Count with category_path
    with_path = sum(1 for cat in categories if cat.get("category_path"))
    print(f"   - Có category_path: {with_path}")
    
    # Step 6: Write back enriched categories
    print(f"\n💾 Đang ghi file enriched: {categories_file}")
    with open(categories_file, "w", encoding="utf-8") as f:
        json.dump(categories, f, ensure_ascii=False, indent=2)
    
    print("✅ Đã ghi xong!")
    
    # Print sample categories
    print("\n📝 Vài categories mẫu:")
    for cat in categories[:3]:
        print(f"\n  • {cat.get('name')}")
        print(f"    - URL: {cat.get('url')}")
        print(f"    - ID: {cat.get('category_id')}")
        print(f"    - Parent: {cat.get('parent_url')}")
        if cat.get("category_path"):
            path_str = " > ".join(cat.get("category_path", []))
            print(f"    - Path: {path_str}")
        print(f"    - Level: {cat.get('level')}")


if __name__ == "__main__":
    # Default path
    categories_file = Path("data/raw/categories_recursive_optimized.json")
    
    enrich_categories_with_paths(categories_file)
    
    print("\n" + "=" * 70)
    print("✅ Hoàn thành!")
    print("=" * 70)
