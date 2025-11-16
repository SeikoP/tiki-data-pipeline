"""
Extract categories từ categories_tree.json và flatten thành flat list
"""

import json
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Import utility để extract category_id
try:
    from ..crawl.utils import extract_category_id_from_url
except ImportError:
    import re

    def extract_category_id_from_url(url: str) -> str | None:
        """Extract category ID từ URL Tiki"""
        if not url:
            return None
        match = re.search(r"/c(\d+)", url)
        if match:
            return f"c{match.group(1)}"
        return None


def flatten_category_tree(
    tree: list[dict[str, Any]], parent_url: str | None = None
) -> list[dict[str, Any]]:
    """
    Flatten category tree thành flat list

    Args:
        tree: List các category nodes (có thể có children)
        parent_url: URL của parent category (None cho root, dùng để override nếu cần)

    Returns:
        List các categories đã flatten
    """
    categories = []

    for node in tree:
        # Extract category_id từ URL nếu chưa có
        category_id = node.get("category_id")
        if not category_id:
            category_id = extract_category_id_from_url(node.get("url", ""))

        # Xác định parent_url: ưu tiên từ node, nếu không có thì dùng parent_url truyền vào
        node_parent_url = node.get("parent_url")
        final_parent_url = node_parent_url if node_parent_url else parent_url

        # Tạo category object
        category = {
            "category_id": category_id,
            "name": node.get("name", ""),
            "url": node.get("url", ""),
            "image_url": node.get("image_url", ""),
            "parent_url": final_parent_url,
            "level": node.get("level", 0),
            "product_count": 0,  # Sẽ được update sau khi load products
        }

        categories.append(category)

        # Recursively flatten children
        # Truyền url của node hiện tại làm parent_url cho children
        children = node.get("children", [])
        if children:
            child_categories = flatten_category_tree(children, parent_url=category["url"])
            categories.extend(child_categories)

    return categories


def extract_categories_from_tree_file(
    tree_file: str | Path,
) -> list[dict[str, Any]]:
    """
    Extract và flatten categories từ file categories_tree.json

    Args:
        tree_file: Đường dẫn đến file categories_tree.json

    Returns:
        List các categories đã flatten
    """
    tree_path = Path(tree_file)
    if not tree_path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {tree_file}")

    logger.info(f"📖 Đang đọc file: {tree_file}")
    with open(tree_path, encoding="utf-8") as f:
        tree = json.load(f)

    if not isinstance(tree, list):
        raise ValueError(f"File {tree_file} không đúng format (phải là list)")

    logger.info(f"✓ Đã đọc {len(tree)} root categories")
    logger.info("🔨 Đang flatten tree structure...")

    categories = flatten_category_tree(tree)
    logger.info(f"✅ Đã flatten thành {len(categories)} categories")

    return categories


if __name__ == "__main__":
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Test extract
    tree_file = "data/raw/categories_tree.json"
    try:
        categories = extract_categories_from_tree_file(tree_file)
        print(f"\n✅ Đã extract {len(categories)} categories")
        print("\n📊 Sample categories (5 đầu tiên):")
        for i, cat in enumerate(categories[:5], 1):
            print(f"  {i}. {cat['name']} (Level {cat['level']}) - {cat['url']}")
    except Exception as e:
        logger.error(f"❌ Lỗi: {e}", exc_info=True)
