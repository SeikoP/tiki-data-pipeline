import json
import os
import sys
from pathlib import Path

# Add src to sys.path
src_path = Path(__file__).resolve().parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

# Mock get_variable and get_int_variable
def get_variable(key, default=None):
    return os.getenv(key, default)

def get_int_variable(key, default=0):
    return int(os.getenv(key, default))

def test_leaf_filtering():
    categories_file = "data/raw/categories_recursive_optimized.json"
    
    if not os.path.exists(categories_file):
        print(f"❌ File not found: {categories_file}")
        return

    with open(categories_file, encoding="utf-8") as f:
        categories = json.load(f)

    print(f"📊 Total categories loaded: {len(categories)}")

    # Implementation logic
    parent_urls = {cat.get("parent_url") for cat in categories if cat.get("parent_url")}
    
    for cat in categories:
        cat["is_leaf"] = cat.get("url") not in parent_urls

    # 1. Check specific category c4556 (Quần lót nam)
    c4556 = next((cat for cat in categories if "c4556" in cat.get("url", "")), None)
    if c4556:
        print(f"🔍 Category c4556 ('{c4556.get('name')}'): is_leaf = {c4556.get('is_leaf')}")
        if not c4556.get('is_leaf'):
            print("✅ c4556 correctly identified as NON-LEAF")
        else:
            print("❌ c4556 incorrectly identified as LEAF")
    else:
        print("⚠️ c4556 not found in categories file")

    # 2. Check children of c4556
    children_of_4556 = [cat for cat in categories if cat.get("parent_url") == "https://tiki.vn/quan-lot-nam/c4556"]
    print(f"🔍 Found {len(children_of_4556)} children for c4556")
    for child in children_of_4556:
        print(f"   - {child.get('name')} ({child.get('url')}): is_leaf = {child.get('is_leaf')}")

    # 3. Simulate filtering
    leaf_only_categories = [cat for cat in categories if cat.get("is_leaf")]
    print(f"🍃 Leaf-only categories count: {len(leaf_only_categories)}")
    
    if c4556 and any(cat.get("url") == c4556.get("url") for cat in leaf_only_categories):
        print("❌ ERROR: c4556 (non-leaf) included in leaf-only list")
    else:
        print("✅ SUCCESS: c4556 excluded from leaf-only list")

if __name__ == "__main__":
    test_leaf_filtering()
