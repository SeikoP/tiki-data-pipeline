import json
import os
import sys
from pathlib import Path


def load_categories(json_file_path: str):
    """
    Load data from categories JSON file to DB using Bulk Copy.
    Re-creates table if needed.
    """
    from pipelines.crawl.storage.postgres_storage import PostgresStorage

    print(f"📂 Reading categories from: {json_file_path}")

    if not os.path.exists(json_file_path):
        print(f"❌ File not found: {json_file_path}")
        return

    try:
        # 1. Read JSON
        with open(json_file_path, encoding="utf-8") as f:
            categories = json.load(f)

        if not categories:
            print("⚠️  File is empty.")
            return

        print(f"📊 Found {len(categories)} categories.")

        # 2. CRITICAL: Đảm bảo tất cả parent categories được include
        # Build URL -> category map từ file JSON đầy đủ
        url_to_cat_full = {cat.get("url"): cat for cat in categories}

        # Tìm tất cả parent URLs cần thiết cho các leaf categories có products
        storage = PostgresStorage()
        used_category_ids = set()
        try:
            used_category_ids = storage.get_used_category_ids()
            print(f"🔍 Found {len(used_category_ids)} active categories in products table")
        except Exception as e:
            print(f"⚠️  Could not get used category IDs: {e}")

        # CRITICAL: Nếu không có products (DB mới), load tất cả leaf categories và parents
        # Nếu có products, chỉ load leaf categories có products và parents của chúng
        parent_urls_needed = set()
        leaf_categories_to_load = []

        # Xác định leaf categories
        parent_urls_in_list = {c.get("parent_url") for c in categories if c.get("parent_url")}

        for cat in categories:
            cat_id = cat.get("category_id")
            if not cat_id and cat.get("url"):
                import re

                match = re.search(r"c?(\d+)", cat.get("url", ""))
                if match:
                    cat_id = f"c{match.group(1)}"

            # Check if leaf category (không có children trong danh sách)
            is_leaf = cat.get("url") not in parent_urls_in_list

            # Nếu không có products, load tất cả leaf categories
            # Nếu có products, chỉ load leaf categories có products
            should_load = False
            if not used_category_ids:
                # DB mới, load tất cả leaf categories
                should_load = is_leaf
            else:
                # Có products, chỉ load leaf categories có products
                should_load = is_leaf and cat_id and cat_id in used_category_ids

            if should_load:
                leaf_categories_to_load.append(cat)
                # Traverse up để collect TẤT CẢ parent URLs lên đến root
                current = cat
                visited = set()
                depth = 0
                while current and depth < 10:
                    parent_url = current.get("parent_url")
                    if not parent_url:
                        # Đã đến root, dừng lại
                        break
                    if parent_url in visited:
                        # Circular reference, dừng lại
                        break
                    visited.add(parent_url)
                    parent_urls_needed.add(parent_url)

                    # Tìm parent trong url_to_cat_full
                    if parent_url in url_to_cat_full:
                        current = url_to_cat_full[parent_url]
                    else:
                        # Parent không có trong file JSON
                        print(f"⚠️  Parent {parent_url} không có trong file JSON")
                        break
                    depth += 1

        # Include tất cả parent categories vào danh sách categories để load
        # CRITICAL: Đảm bảo traverse đầy đủ để include cả parent của parent
        categories_to_load = list(leaf_categories_to_load)
        missing_parents = []

        # Traverse đệ quy để include TẤT CẢ parent categories
        parent_urls_to_check = set(parent_urls_needed)
        while parent_urls_to_check:
            current_batch = set(parent_urls_to_check)
            parent_urls_to_check = set()

            for parent_url in current_batch:
                if parent_url in url_to_cat_full:
                    parent_cat = url_to_cat_full[parent_url]
                    # Chỉ thêm nếu chưa có trong danh sách
                    if not any(c.get("url") == parent_url for c in categories_to_load):
                        categories_to_load.append(parent_cat)
                        # Kiểm tra parent của parent này
                        grandparent_url = parent_cat.get("parent_url")
                        if grandparent_url and grandparent_url not in parent_urls_needed:
                            parent_urls_needed.add(grandparent_url)
                            parent_urls_to_check.add(grandparent_url)
                else:
                    missing_parents.append(parent_url)

        if missing_parents:
            print(f"⚠️  Cảnh báo: {len(missing_parents)} parent URLs không có trong file JSON:")
            for url in missing_parents[:5]:
                print(f"   - {url}")

        print(
            f"📂 Sẽ load {len(categories_to_load)} categories ({len(leaf_categories_to_load)} leaves + {len(parent_urls_needed)} parents)"
        )

        # Debug: In ra danh sách categories sẽ load (chỉ hiển thị một số)
        if len(categories_to_load) <= 20:
            print("\n📋 Danh sách categories sẽ load:")
            for cat in categories_to_load:
                print(f"   - [{cat.get('level', '?')}] {cat.get('name')} ({cat.get('url')})")
        else:
            print(f"\n📋 Sẽ load {len(categories_to_load)} categories (quá nhiều để hiển thị)")

        # 3. Connect & Save
        print("🚀 Importing to Database...")

        # Load categories với đầy đủ parent hierarchy
        saved_count = storage.save_categories(
            categories_to_load,
            only_leaf=False,  # Load cả parents để đảm bảo path đầy đủ
            sync_with_products=False,  # Đã filter ở trên rồi
        )

        print(
            f"✅ DONE! Successfully loaded {saved_count} categories (including parent categories)."
        )
        print("ℹ️  Table 'categories' relies on 'url' as Primary Key.")

        # Update product_count from actual products in database
        print("📊 Updating product_count from products table...")
        updated_count = storage.update_category_product_counts()
        print(f"✅ Updated product_count for {updated_count} categories.")

    except Exception as e:
        print(f"❌ Error during load: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    # Side-effects only when run as a script
    current_dir = Path(__file__).resolve().parent
    src_dir = current_dir.parent.parent
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    # Config DB host for local run
    if "POSTGRES_HOST" not in os.environ:
        print("ℹ️  Setting POSTGRES_HOST=localhost for local execution")
        os.environ["POSTGRES_HOST"] = "localhost"

    # Default path based on user's structure
    default_path = os.path.join(
        src_dir.parent, "data", "raw", "categories_recursive_optimized.json"
    )

    # Allow command line arg override
    target_file = sys.argv[1] if len(sys.argv) > 1 else default_path

    load_categories(target_file)
