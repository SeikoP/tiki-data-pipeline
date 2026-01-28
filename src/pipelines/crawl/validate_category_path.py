#!/usr/bin/env python
"""Validate and fix category_path to ensure parent category (Level 0) is always at index 0.

Vấn đề: Một số products có category_path mà danh mục level 1 đang ở index 0 thay vì parent category.
Giải pháp: Validate và tự động fix để đảm bảo parent category luôn ở index 0.
"""

import logging
import os
from typing import Any

from .config import MAX_CATEGORY_LEVELS
from .crawl_products_detail import (
    get_parent_category_name,
    load_category_hierarchy,
)

logger = logging.getLogger(__name__)

# Get default parent category name from environment or fallback
PARENT_CATEGORY_NAME = os.getenv("TIKI_DEFAULT_ROOT_CATEGORY", "Nhà Cửa - Đời Sống")


def validate_and_fix_category_path(
    category_path: list[str] | None,
    category_url: str | None = None,
    hierarchy_map: dict[str, Any] | None = None,
    auto_fix: bool = True,
) -> list[str]:
    """Validate và fix category_path để đảm bảo parent category (Level 0) ở index 0.

    Args:
        category_path: Danh sách category names (có thể None hoặc empty)
        category_url: URL của category (optional, để lookup parent)
        hierarchy_map: Pre-loaded hierarchy map (optional)
        auto_fix: Nếu True, tự động fix nếu thiếu parent category

    Returns:
        list[str]: Category path đã được fix (hoặc original nếu không cần fix)
    """
    # Handle None hoặc empty
    if not category_path or not isinstance(category_path, list):
        return []

    # Clean empty strings
    category_path = [c.strip() for c in category_path if c and isinstance(c, (str, int, float))]
    if not category_path:
        return []

    # Nếu path rỗng hoặc chỉ có 1-2 items, có thể không cần fix
    if len(category_path) < 2:
        return category_path[:MAX_CATEGORY_LEVELS]

    # Kiểm tra xem item đầu tiên có phải là parent category không
    first_item = category_path[0]

    # Kiểm tra xem first_item có phải là một Level 0 category trong hierarchy map không
    is_known_root = False
    if hierarchy_map:
        # Tìm xem có category nào có name == first_item và level == 0 không
        is_known_root = any(
            info.get("name") == first_item and info.get("level", 1) == 0
            for info in hierarchy_map.values()
        )

    # Nếu item đầu tiên đã là parent category (hoặc là một root đã biết), không cần fix
    if first_item == PARENT_CATEGORY_NAME or is_known_root:
        # Đảm bảo không vượt quá MAX_CATEGORY_LEVELS
        return category_path[:MAX_CATEGORY_LEVELS]

    # Nếu auto_fix = False, chỉ validate và return original
    if not auto_fix:
        logger.warning(
            f"Category path missing parent category at index 0. First item: {first_item}"
        )
        return category_path[:MAX_CATEGORY_LEVELS]

    # Tìm parent category cho item đầu tiên
    parent_name = None

    # Option 1: Nếu có category_url, dùng để lookup
    if category_url:
        try:
            if hierarchy_map is None:
                hierarchy_map = load_category_hierarchy()
            if hierarchy_map:
                parent_name = get_parent_category_name(category_url, hierarchy_map)
        except Exception as e:
            logger.debug(f"Error getting parent from category_url: {e}")

    # Option 2: Nếu không có category_url, thử tìm parent từ hierarchy bằng name
    if not parent_name and hierarchy_map:
        try:
            # Build reverse lookup: name -> url
            name_to_url = {info.get("name"): url for url, info in hierarchy_map.items()}

            if first_item in name_to_url:
                cat_url = name_to_url[first_item]
                parent_name = get_parent_category_name(cat_url, hierarchy_map)
        except Exception as e:
            logger.debug(f"Error getting parent from hierarchy by name: {e}")

    # Option 3: Nếu vẫn không tìm được, sử dụng default parent category
    if not parent_name:
        parent_name = PARENT_CATEGORY_NAME
        logger.debug(
            f"Could not find parent category for '{first_item}', using default: {parent_name}"
        )

    # Fix: Prepend parent category nếu chưa có
    if parent_name and parent_name != first_item:
        # Remove parent_name nếu đã có trong path (tránh duplicate)
        filtered_path = [c for c in category_path if c != parent_name]
        fixed_path = [parent_name] + filtered_path

        # Đảm bảo không vượt quá MAX_CATEGORY_LEVELS
        if len(fixed_path) > MAX_CATEGORY_LEVELS:
            fixed_path = fixed_path[:MAX_CATEGORY_LEVELS]

        logger.debug(f"Fixed category_path: {category_path[:3]}... -> {fixed_path[:3]}...")
        return fixed_path

    # Nếu không cần fix, chỉ đảm bảo không vượt quá MAX
    return category_path[:MAX_CATEGORY_LEVELS]


def fix_product_category_path(
    product: dict[str, Any],
    hierarchy_map: dict[str, Any] | None = None,
    auto_fix: bool = True,
) -> dict[str, Any]:
    """Fix category_path cho một product.

    Args:
        product: Product dictionary
        hierarchy_map: Pre-loaded hierarchy map (optional)
        auto_fix: Nếu True, tự động fix

    Returns:
        dict: Product với category_path đã được fix
    """
    category_path = product.get("category_path")
    category_url = product.get("category_url")

    if category_path:
        fixed_path = validate_and_fix_category_path(
            category_path, category_url, hierarchy_map, auto_fix
        )
        product["category_path"] = fixed_path

    return product


def fix_products_category_paths(
    products: list[dict[str, Any]],
    hierarchy_map: dict[str, Any] | None = None,
    auto_fix: bool = True,
) -> list[dict[str, Any]]:
    """Fix category_path cho danh sách products.

    Args:
        products: List of product dictionaries
        hierarchy_map: Pre-loaded hierarchy map (optional)
        auto_fix: Nếu True, tự động fix

    Returns:
        list: Products với category_path đã được fix
    """
    if not products:
        return products

    # Load hierarchy map một lần nếu chưa có
    if hierarchy_map is None and auto_fix:
        try:
            hierarchy_map = load_category_hierarchy()
        except Exception as e:
            logger.warning(f"Could not load hierarchy map: {e}")

    fixed_products = []
    fixed_count = 0

    for product in products:
        original_path = product.get("category_path")
        fixed_product = fix_product_category_path(product, hierarchy_map, auto_fix)
        fixed_path = fixed_product.get("category_path")

        # Track số lượng products đã được fix
        if original_path != fixed_path:
            fixed_count += 1

        fixed_products.append(fixed_product)

    if fixed_count > 0:
        logger.info(f"Fixed category_path for {fixed_count}/{len(products)} products")

    return fixed_products
