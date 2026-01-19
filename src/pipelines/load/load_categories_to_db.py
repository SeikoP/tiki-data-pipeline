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

        # 2. Connect & Save
        storage = PostgresStorage()
        print("🚀 Importing to Database...")

        # Load only categories that have products + their parent hierarchy
        # This ensures clean data while preserving complete category paths
        saved_count = storage.save_categories(
            categories, 
            only_leaf=True,  # Include only leaf categories as requested
            sync_with_products=True  # Smart filter: leaves with products + parents
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
