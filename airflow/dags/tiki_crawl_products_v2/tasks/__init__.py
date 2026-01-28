"""Task callables split by domain.

This package re-exports all task callables to preserve backward compatibility with
`from tiki_crawl_products_v2.tasks import *`.
"""

from __future__ import annotations

from .common import get_logger, _fix_sys_path_for_pipelines_import, atomic_write_file
from .category import load_categories, crawl_single_category
from .product import merge_products, save_products, transform_products
from .detail import (
    prepare_products_for_detail,
    crawl_product_batch,
    crawl_single_product_detail,
    merge_product_details,
    save_products_with_detail,
)
from .loader import (
    fix_missing_parent_categories,
    load_categories_to_db_wrapper,
    _import_postgres_storage,
    load_products,
)
from .maintenance import (
    cleanup_incomplete_products_wrapper,
    cleanup_orphan_categories_wrapper,
    cleanup_redundant_categories_wrapper,
    reconcile_categories_wrapper,
    cleanup_old_history_wrapper,
    validate_data,
    cleanup_redis_cache,
    backup_database,
)
from .monitoring import aggregate_and_notify

__all__ = [
    'get_logger', '_fix_sys_path_for_pipelines_import', 'atomic_write_file',
    'load_categories', 'crawl_single_category',
    'merge_products', 'save_products', 'transform_products',
    'prepare_products_for_detail', 'crawl_product_batch', 'crawl_single_product_detail',
    'merge_product_details', 'save_products_with_detail',
    'fix_missing_parent_categories', 'load_categories_to_db_wrapper',
    '_import_postgres_storage', 'load_products',
    'cleanup_incomplete_products_wrapper', 'cleanup_orphan_categories_wrapper',
    'cleanup_redundant_categories_wrapper', 'reconcile_categories_wrapper',
    'cleanup_old_history_wrapper', 'validate_data', 'cleanup_redis_cache',
    'backup_database', 'aggregate_and_notify',
]
