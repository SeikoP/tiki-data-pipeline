import os
import json
import logging
from typing import Dict, Any

# Cache hierarchy map globally to avoid reloading in every task
_hierarchy_map_cache = None

def get_hierarchy_map(force_reload: bool = False) -> Dict[str, Any]:
    """Load category hierarchy map for auto-parent-detection
    
    This map contains all categories with their parent chains,
    allowing extract_product_detail to auto-detect missing Level 0 (parent category)
    """
    global _hierarchy_map_cache

    if _hierarchy_map_cache is not None and not force_reload:
        return _hierarchy_map_cache

    try:
        # Try multiple potential paths
        possible_paths = [
            "/opt/airflow/data/raw/category_hierarchy_map.json",
            os.path.join(os.getcwd(), "data", "raw", "category_hierarchy_map.json"),
        ]

        hierarchy_file = None
        for path in possible_paths:
            if os.path.exists(path):
                hierarchy_file = path
                break

        if hierarchy_file:
            with open(hierarchy_file, encoding="utf-8") as f:
                _hierarchy_map_cache = json.load(f)
                logging.info(
                    f"✅ Loaded category hierarchy map: {len(_hierarchy_map_cache)} categories"
                )
                return _hierarchy_map_cache
        else:
            logging.warning(f"⚠️  Hierarchy map not found. Checked: {possible_paths}")
            return {}
    except Exception as e:
        logging.error(f"❌ Error loading hierarchy map: {e}")
        return {}
