# Archived Tests

This directory contains deprecated test files that are no longer actively maintained but kept for historical reference.

## 📦 **Archived Tests**

| File | Original Purpose | Archived Date | Reason |
|------|------------------|---------------|--------|
| `check_sales_count_in_data.py` | One-time validation of sales_count field | Nov 19, 2025 | One-time check, no longer needed |
| `final_validation.py` | Phase completion validation | Nov 19, 2025 | Development phase completed |
| `test_phase4_complete.py` | Phase 4 optimization validation | Nov 19, 2025 | Phase 4 completed |
| `test_phase4_loader.py` | Loader performance benchmark | Nov 19, 2025 | Replaced by active performance tests |
| `validate_optimized_dag.py` | One-time DAG validation | Nov 19, 2025 | DAG validated, no longer needed |
| `test_quick_crawl.py` | Quick crawl prototype | Nov 19, 2025 | Prototype, replaced by production code |
| `test_performance_optimizations.py` | Performance optimization tests | Nov 19, 2025 | Replaced by newer benchmark tests |

---

## 🔍 **Why These Tests Were Archived**

### **One-Time Validation Scripts**
- `check_sales_count_in_data.py` - Used once to verify field exists
- `validate_optimized_dag.py` - Used once to validate DAG structure
- `final_validation.py` - Used at end of development phase

### **Phase-Specific Tests**
- `test_phase4_complete.py` - Validated Phase 4 completion
- `test_phase4_loader.py` - Benchmarked old vs new loader (now obsolete)

### **Prototype/Experimental Code**
- `test_quick_crawl.py` - Early prototype, replaced by production crawlers
- `test_performance_optimizations.py` - Old performance tests, newer ones exist

---

## 📝 **Notes**

- These files are kept for reference only
- Do not use in CI/CD pipelines
- May not work with current codebase
- For historical purposes only

---

## 🔄 **Restoration**

If you need to restore any test:
```bash
# Move back to tests directory
Move-Item tests/archive/<filename> tests/<filename>
```

---

**Archived:** November 19, 2025  
**By:** GitHub Copilot AI Assistant
