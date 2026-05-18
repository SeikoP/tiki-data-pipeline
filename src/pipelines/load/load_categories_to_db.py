import json
import logging
import os
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def _extract_products(payload):
    if isinstance(payload, list):
        return payload
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("products"), list):
        return payload["products"]
    data = payload.get("data")
    if isinstance(data, dict) and isinstance(data.get("products"), list):
        return data["products"]
    return []


def _used_category_ids_from_products(product_file_path: str | None) -> set[str]:
    if not product_file_path or not os.path.exists(product_file_path):
        return set()

    from common.category_utils import normalize_category_id

    with open(product_file_path, encoding="utf-8") as f:
        products = _extract_products(json.load(f))

    used_category_ids = set()
    for product in products:
        if not isinstance(product, dict):
            continue
        category_id = normalize_category_id(
            product.get("category_id"),
            product.get("category_url"),
        )
        if category_id:
            used_category_ids.add(category_id)
    return used_category_ids


def _select_categories_for_product_refs(
    categories: list[dict], used_category_ids: set[str]
) -> list[dict]:
    """Return categories referenced by current products plus their parent chain."""
    from common.category_utils import normalize_category_tree

    normalized = normalize_category_tree(categories)
    id_to_cat = {cat.get("category_id"): cat for cat in normalized if cat.get("category_id")}
    url_to_cat = {cat.get("url"): cat for cat in normalized if cat.get("url")}

    selected_by_url: dict[str, dict] = {}
    missing_category_ids = sorted(cat_id for cat_id in used_category_ids if cat_id not in id_to_cat)
    if missing_category_ids:
        raise ValueError(
            "Product categories missing from category JSON: "
            + ", ".join(missing_category_ids[:20])
        )

    for cat_id in sorted(used_category_ids):
        current = id_to_cat.get(cat_id)
        visited: set[str] = set()
        while current:
            current_url = current.get("url")
            if not current_url or current_url in visited:
                break
            visited.add(current_url)
            selected_by_url[current_url] = current

            parent_url = current.get("parent_url")
            current = url_to_cat.get(parent_url) if parent_url else None

    return list(selected_by_url.values())


def load_categories(json_file_path: str, product_file_path: str | None = None):
    """Load data from categories JSON file to DB using Bulk Copy.

    If product_file_path is provided, only categories referenced by current-run products
    plus their parent chain are loaded.
    """
    from pipelines.crawl.storage.postgres_storage import PostgresStorage

    logger.info(f"📂 Reading categories from: {json_file_path}")

    if not os.path.exists(json_file_path):
        logger.info(f"❌ File not found: {json_file_path}")
        return

    try:
        # 1. Read JSON
        with open(json_file_path, encoding="utf-8") as f:
            categories = json.load(f)

        if not categories:
            logger.info("⚠️  File is empty.")
            return

        logger.info(f"📊 Found {len(categories)} categories.")

        # Tìm tất cả parent URLs cần thiết cho leaf categories có product trong current run.
        # Không lọc theo bảng products cũ trong DB: task này chạy trước khi load products
        # của current run, nên product categories mới sẽ chưa tồn tại trong DB.
        storage = PostgresStorage()
        used_category_ids = _used_category_ids_from_products(product_file_path)
        if used_category_ids:
            logger.info(
                f"🔍 Found {len(used_category_ids)} categories from current product file: {product_file_path}"
            )
        else:
            logger.info(
                "⚠️  No current product category IDs found; loading all leaf categories as fallback"
            )

        if used_category_ids:
            categories_to_load = _select_categories_for_product_refs(categories, used_category_ids)
        else:
            from common.category_utils import normalize_category_tree

            normalized_categories = normalize_category_tree(categories)
            categories_to_load = [
                cat for cat in normalized_categories if cat.get("is_leaf")
            ]

        logger.info(
            "Sẽ load %s categories theo current product refs",
            len(categories_to_load),
        )

        # Debug: In ra danh sách categories sẽ load (chỉ hiển thị một số)
        if len(categories_to_load) <= 20:
            logger.info("\n📋 Danh sách categories sẽ load:")
            for cat in categories_to_load:
                logger.info(f"   - [{cat.get('level', '?')}] {cat.get('name')} ({cat.get('url')})")
        else:
            logger.info(
                f"\n📋 Sẽ load {len(categories_to_load)} categories (quá nhiều để hiển thị)"
            )

        # 3. Connect & Save
        logger.info("🚀 Importing to Database...")

        # Load categories với đầy đủ parent hierarchy
        saved_count = storage.save_categories(
            categories_to_load,
            only_leaf=False,
            sync_with_products=False,
        )

        logger.info(
            f"DONE! Successfully loaded {saved_count} categories (including parent categories)."
        )
        logger.info("ℹ️  Table 'categories' relies on 'url' as Primary Key.")

        # Update product_count from actual products in database
        logger.info("📊 Updating product_count from products table...")
        updated_count = storage.update_category_product_counts()
        logger.info(f"✅ Updated product_count for {updated_count} categories.")
        return {"status": "success", "count": saved_count}

    except Exception as e:
        logger.error(f"❌ Error during load: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Side-effects only when run as a script
    current_dir = Path(__file__).resolve().parent
    src_dir = current_dir.parent.parent
    if str(src_dir) not in sys.path:
        sys.path.insert(0, str(src_dir))

    # Config DB host for local run
    if "POSTGRES_HOST" not in os.environ:
        logger.info("ℹ️  Setting POSTGRES_HOST=localhost for local execution")
        os.environ["POSTGRES_HOST"] = "localhost"

    # Default path based on user's structure
    default_path = os.path.join(
        src_dir.parent, "data", "raw", "categories_recursive_optimized.json"
    )

    # Allow command line arg override
    target_file = sys.argv[1] if len(sys.argv) > 1 else default_path

    load_categories(target_file)
