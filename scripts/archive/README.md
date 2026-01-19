# Archived Scripts

**Last Updated:** January 19, 2026

This folder contains scripts that are no longer actively used but kept for reference.

## 📁 Structure

### `one-time-migrations/`
Scripts that were run once to fix/migrate data:
- `migrate_crawl_history.sql` - Schema optimization for crawl_history table
- `check_null_brand_seller.py` - Check for null brand/seller
- `delete_null_brand_seller.py` - Delete records with null brand/seller
- `verify_format.py` - Verify data format

### `debug/`
Debug scripts used during development:
- `debug_hierarchy.py` - Debug category hierarchy
- `test_parent_detection.py` - Test parent detection logic

---

**Note:** These scripts are kept for historical reference. Do not run them unless you understand their purpose.
