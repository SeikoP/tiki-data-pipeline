"""Category normalization helpers shared by Airflow tasks and transforms."""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

CATEGORY_ID_RE = re.compile(r"/c(\d+)(?:[/?#]|$)")


def extract_category_id_from_url(url: str | None) -> str | None:
    if not url:
        return None
    match = CATEGORY_ID_RE.search(str(url))
    if not match:
        return None
    return f"c{match.group(1)}"


def normalize_category_id(category_id: Any, url: str | None = None) -> str | None:
    if category_id:
        raw = str(category_id).strip()
        if not raw:
            return extract_category_id_from_url(url)
        return raw if raw.startswith("c") else f"c{raw}"
    return extract_category_id_from_url(url)


def normalize_category_tree(categories: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return categories with category_id, is_leaf, category_path and level fields normalized."""
    normalized = [deepcopy(cat) for cat in categories]
    url_to_cat = {cat.get("url"): cat for cat in normalized if cat.get("url")}
    parent_urls = {cat.get("parent_url") for cat in normalized if cat.get("parent_url")}

    for cat in normalized:
        cat["category_id"] = normalize_category_id(cat.get("category_id"), cat.get("url"))
        cat["is_leaf"] = cat.get("url") not in parent_urls

    for cat in normalized:
        path: list[str] = []
        current = cat
        visited: set[str] = set()
        while current:
            current_url = current.get("url")
            if current_url in visited:
                break
            if current_url:
                visited.add(current_url)

            name = current.get("name")
            if name:
                path.insert(0, str(name))

            parent_url = current.get("parent_url")
            current = url_to_cat.get(parent_url) if parent_url else None

        if path:
            cat["category_path"] = path[:5]
            for idx in range(5):
                key = f"level_{idx + 1}"
                cat[key] = path[idx] if idx < len(path) else None
            cat["root_category_name"] = path[0]
            cat["level"] = max(int(cat.get("level") or 0), len(path) - 1)

    return normalized


def build_category_lookup(categories: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    normalized = normalize_category_tree(categories)
    lookup: dict[str, dict[str, Any]] = {}
    for cat in normalized:
        cat_id = cat.get("category_id")
        if cat_id:
            lookup[cat_id] = cat
    return lookup


__all__ = [
    "build_category_lookup",
    "extract_category_id_from_url",
    "normalize_category_id",
    "normalize_category_tree",
]
