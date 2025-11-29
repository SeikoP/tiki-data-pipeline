# Verifydata Directory

**Last Updated:** November 29, 2025  
**Status:** ✅ Cleaned & Organized  
**Active Scripts:** 2 files  
**Archived Scripts:** 23 files (moved to `verifydata/archive/`)

---

## 📊 Quick Overview

| Category | Count | Purpose |
|----------|-------|---------|
| **Active Verification** | 2 scripts | Ongoing data quality checks |
| **Archived** | 23 scripts | One-time fixes and validations |

---

## ✅ Active Scripts

### `check_warehouse_data.py`
**Purpose:** Check warehouse (tiki_warehouse) data quality and statistics

**Usage:**
```bash
python verifydata/check_warehouse_data.py
```

**Features:**
- Checks dim_category data
- Verifies category hierarchy
- Displays data distribution statistics

---

### `verify_warehouse_cleaned.py`
**Purpose:** Verify warehouse data after cleaning operations

**Usage:**
```bash
python verifydata/verify_warehouse_cleaned.py
```

**Features:**
- Validates data cleanup results
- Checks for data consistency
- Reports any issues found

---

## 🗑️ Archived Scripts

The following 23 scripts have been moved to `verifydata/archive/` as they were one-time use scripts:

### Check Scripts (7 files)
- `check_breadcrumb.py` - Verify breadcrumb data
- `check_category_format.py` - Check category formatting
- `check_hierarchy.py` - Validate category hierarchy
- `check_incomplete_categories.py` - Find incomplete categories
- `check_long_categories.py` - Check for overly long category names
- `check_suspicious_categories.py` - Identify suspicious category data
- `check_xe_day_hang.py` - Specific category check

### Fix Scripts (10 files)
- `fix_all_incomplete_categories.py` - Batch fix incomplete categories
- `fix_category_format.py` - Fix category formatting issues
- `fix_category_path_final.py` - Final category path corrections
- `fix_category_path.py` - Category path fixes
- `fix_missing_level4_update.py` - Update missing level 4 categories
- `fix_missing_level4.py` - Fix missing level 4 categories
- `fix_remaining_product_names.py` - Clean up product names
- `fix_source_category_format.py` - Fix source category format
- `fix_suspicious_categories.py` - Fix suspicious category data
- `fix_three_level_products.py` - Fix 3-level product categorization

### Test/Validation Scripts (6 files)
- `test_auto_parent_detection.py` - Test parent category detection
- `validate_dag_hardening.py` - Validate DAG hardening changes
- `verify_fix_quality.py` - Verify data fix quality
- `verify_level4_fix.py` - Verify level 4 category fixes
- `verify_warehouse_hierarchy.py` - Verify warehouse hierarchy
- `verify_warehouse_with_path.py` - Verify warehouse path data

---

## 📝 Usage Guidelines

### When to Use Active Scripts

**Use `check_warehouse_data.py` when:**
- After running ETL pipeline
- Before generating reports
- Monitoring data quality
- Investigating data issues

**Use `verify_warehouse_cleaned.py` when:**
- After data cleaning operations
- Validating data migration
- Ensuring data consistency

---

### When to Reference Archived Scripts

**Reference archived scripts when:**
- Investigating historical data issues
- Understanding past data fixes
- Debugging similar problems
- Learning from past approaches

**Note:** Archived scripts are kept for reference but should not be run in production.

---

## 🔧 Maintenance

### Adding New Verification Scripts

**Guidelines:**
1. Keep scripts focused on one verification task
2. Add clear documentation and usage examples
3. Include expected output examples
4. Handle errors gracefully
5. Log results clearly

**Script Template:**
```python
#!/usr/bin/env python
"""
Verify <aspect> of data

Purpose: <what this script verifies>
Usage: python verifydata/<script_name>.py
"""

import psycopg2
from dotenv import load_dotenv
import os

def main():
    print("=" * 70)
    print("VERIFICATION: <Title>")
    print("=" * 70)
    
    # Verification logic here
    
    print("\n✅ Verification completed!")

if __name__ == "__main__":
    main()
```

---

### Archiving Old Scripts

**When to archive:**
- Script was used for one-time fix
- Problem no longer exists
- Script superseded by better approach
- Script specific to past data issue

**How to archive:**
```powershell
Move-Item -Path "verifydata/<script>.py" -Destination "verifydata/archive/"
```

**Update this README after archiving!**

---

## 🆘 Troubleshooting

### Database Connection Errors
```bash
# Check PostgreSQL is running
docker-compose ps postgres

# Check environment variables
cat .env | grep POSTGRES
```

### Import Errors
```bash
# Run from project root
cd e:\Project\tiki-data-pipeline
python verifydata/<script>.py
```

---

## 📚 Related Documentation

- **Database Schema:** `docs/03-ARCHITECTURE/database-schema.md`
- **Data Quality:** `docs/06-ANALYSIS/data-quality-report.md`
- **Scripts:** `scripts/README.md`

---

**Last Updated:** November 29, 2025  
**Maintained By:** GitHub Copilot AI Assistant
