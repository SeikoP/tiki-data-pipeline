"""Thin shim to keep DAG import path stable.

The original monolithic DAG was split into `tiki_crawl_products_v2/`.
"""

from __future__ import annotations

from tiki_crawl_products_v2.dag import dag  # noqa: F401
