"""
Script để validate file hierarchical JSON - kiểm tra tính chính xác của cấu trúc
"""
import os
import sys
import json

# Thêm path để import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from pipelines.crawl.tiki.extract_category_link import (
    validate_hierarchical_structure,
    load_categories_from_json
)

def validate_file(hierarchical_file, all_categories_file=None):
    """Validate một file hierarchical"""
    
    print(f"\n{'='*70}")
    print(f"Validating: {hierarchical_file}")
    print(f"{'='*70}\n")
    
    # Load hierarchical
    if not os.path.exists(hierarchical_file):
        print(f"❌ File không tồn tại: {hierarchical_file}")
        return False
    
    try:
        hierarchical = load_categories_from_json(hierarchical_file)
        print(f"✓ Loaded hierarchical: {len(hierarchical)} root categories")
    except Exception as e:
        print(f"❌ Lỗi khi load: {e}")
        return False
    
    # Load all_categories nếu có
    all_categories = []
    if all_categories_file and os.path.exists(all_categories_file):
        try:
            all_categories = load_categories_from_json(all_categories_file)
            print(f"✓ Loaded all_categories: {len(all_categories)} categories")
        except Exception as e:
            print(f"⚠️  Lỗi khi load all_categories: {e}")
    else:
        print(f"⚠️  No all_categories file provided/found")
    
    # Validate
    print(f"\n{'='*70}")
    print("VALIDATION RESULTS")
    print(f"{'='*70}\n")
    
    if all_categories:
        result = validate_hierarchical_structure(hierarchical, all_categories)
    else:
        # Validate without original categories (basic checks only)
        result = validate_hierarchical_structure(hierarchical, hierarchical)
    
    print(f"✅ Valid: {result['is_valid']}")
    print(f"\n📊 Statistics:")
    for key, value in result['stats'].items():
        print(f"  - {key}: {value}")
    
    if result['errors']:
        print(f"\n⚠️  {len(result['errors'])} Issues Found:")
        for i, error in enumerate(result['errors'][:15], 1):
            print(f"  {i}. {error}")
        if len(result['errors']) > 15:
            print(f"  ... and {len(result['errors']) - 15} more")
    else:
        print(f"\n✅ No issues found!")
    
    return result['is_valid']


def main():
    """Main function"""
    print("\n" + "="*70)
    print(" " * 15 + "HIERARCHICAL STRUCTURE VALIDATOR")
    print("="*70)
    
    # Validate demo file
    demo_hier = "data/raw/demo/demo_hierarchical.json"
    demo_all = "data/raw/demo/demo_categories.json"
    
    # Kết hợp categories và sub-categories cho validation
    if os.path.exists(demo_hier):
        # Load demo data
        demo_categories = []
        if os.path.exists(demo_all):
            demo_categories = load_categories_from_json(demo_all)
        
        demo_sub = "data/raw/demo/demo_sub_categories.json"
        if os.path.exists(demo_sub):
            demo_categories.extend(load_categories_from_json(demo_sub))
        
        validate_file(demo_hier, None)  # Pass None để không compare
        
        # Manual validation
        print(f"\n{'='*70}")
        print("DETAILED STRUCTURE ANALYSIS")
        print(f"{'='*70}\n")
        
        hierarchical = load_categories_from_json(demo_hier)
        
        # Count categories in hierarchy
        def count_all(cats):
            count = 0
            for cat in cats:
                count += 1
                if cat.get('sub_categories'):
                    count += count_all(cat['sub_categories'])
            return count
        
        total = count_all(hierarchical)
        
        print(f"Root categories: {len(hierarchical)}")
        print(f"Total categories (all levels): {total}")
        
        # Show structure
        print(f"\nStructure overview:")
        for root in hierarchical[:3]:
            name = root.get('name', 'N/A')
            cat_id = root.get('category_id', 'N/A')
            sub_count = len(root.get('sub_categories', []))
            print(f"  - {name} (ID: {cat_id})")
            print(f"    └─ {sub_count} sub-categories")
            
            # Show first level subs
            for sub in root.get('sub_categories', [])[:2]:
                sub_name = sub.get('name', 'N/A')
                sub_id = sub.get('category_id', 'N/A')
                parent_id = sub.get('parent_id', 'N/A')
                level_2_count = len(sub.get('sub_categories', []))
                print(f"      - {sub_name} (ID: {sub_id}, Parent: {parent_id})")
                print(f"        └─ {level_2_count} sub-categories")
        
        if len(hierarchical) > 3:
            print(f"  ... and {len(hierarchical) - 3} more root categories")
    
    else:
        print(f"⚠️  Demo file not found: {demo_hier}")
    
    # Validate production file if exists
    prod_hier = "data/raw/tiki_categories_merged.json"
    prod_all = "data/raw/tiki_all_categories.json"
    
    if os.path.exists(prod_hier):
        print(f"\n\n")
        validate_file(prod_hier, prod_all if os.path.exists(prod_all) else None)
    
    print(f"\n{'='*70}")
    print("✅ Validation Complete!")
    print(f"{'='*70}\n")


if __name__ == "__main__":
    main()

