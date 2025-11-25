# Tests Directory

**Last Updated:** November 19, 2025  
**Status:** ✅ Cleaned & Organized  
**Active Tests:** 8 files  
**Archived Tests:** 7 files (moved to `tests/archive/`)

---

## 📊 **Quick Overview**

| Category | Count | Status |
|----------|-------|--------|
| **Core Functionality** | 4 tests | ✅ Active |
| **Performance & Benchmarks** | 1 test | ✅ Active |
| **Integration Tests** | 3 tests | ✅ Active |
| **Archived/Deprecated** | 7 tests | ⚠️ Archived |

**Key Tests:**
- 🔥 `test_cache_hit_rate_fix.py` - Cache optimization (NEW)
- ⚙️ `test_utils.py` - Utility functions
- 🕷️ `test_crawl_products.py` - Product crawling
- 🔄 `test_transform_load.py` - Data transformation

---

## 📋 **Test Categories**

### ✅ **Active & Maintained Tests**

#### 1. **Core Functionality Tests**

| Test File | Purpose | Status | Run Command |
|-----------|---------|--------|-------------|
| `test_cache_hit_rate_fix.py` | Validates cache optimization (10% → 60-80%) | ✅ Active | `python tests/test_cache_hit_rate_fix.py` |
| `test_utils.py` | Tests utility functions (parse_sales_count, parse_price, extract_product_id) | ✅ Active | `python tests/test_utils.py` |
| `test_crawl_products.py` | Tests product crawling functionality | ✅ Active | `python tests/test_crawl_products.py` |
| `test_transform_load.py` | Tests data transformation and loading | ✅ Active | `python tests/test_transform_load.py` |

#### 2. **Performance & Benchmark Tests**

| Test File | Purpose | Status | Run Command |
|-----------|---------|--------|-------------|
| `test_parallel_benchmark.py` | Benchmarks parallel vs sequential crawling | ✅ Active | `python tests/test_parallel_benchmark.py` |
| `test_performance_optimizations.py` | Tests performance optimization features | ✅ Active | `python tests/test_performance_optimizations.py` |

#### 3. **Integration Tests**

| Test File | Purpose | Status | Run Command |
|-----------|---------|--------|-------------|
| `test_sales_count.py` | Validates sales_count extraction in products | ✅ Active | `python tests/test_sales_count.py` |
| `test_crawl_recursive.py` | Tests recursive category crawling | ✅ Active | `python tests/test_crawl_recursive.py` |

---

### ⚠️ **Deprecated / One-Time Use Tests**

These tests were used for specific development phases and are no longer needed:

| Test File | Purpose | Status | Action |
|-----------|---------|--------|--------|
| `check_sales_count_in_data.py` | One-time check for sales_count field | ⚠️ Deprecated | Can be archived |
| `final_validation.py` | Phase completion validation | ⚠️ Deprecated | Can be archived |
| `test_phase4_complete.py` | Phase 4 validation | ⚠️ Deprecated | Can be archived |
| `test_phase4_loader.py` | Phase 4 loader benchmark | ⚠️ Deprecated | Can be archived |
| `validate_optimized_dag.py` | One-time DAG validation | ⚠️ Deprecated | Can be archived |
| `test_quick_crawl.py` | Quick crawl prototype | ⚠️ Deprecated | Can be archived |
| `test_ai_summary_discord.py` | Discord notification test (if not used) | ⚠️ Check | Keep if Discord integration active |

---

## 🔥 **Recommended Test Files to Keep**

### **Core Tests (Must Keep)**
1. ✅ `test_cache_hit_rate_fix.py` - Cache optimization validation
2. ✅ `test_utils.py` - Utility function tests
3. ✅ `test_crawl_products.py` - Crawl functionality
4. ✅ `test_transform_load.py` - Transform/Load pipeline

### **Performance Tests (Recommended)**
5. ✅ `test_parallel_benchmark.py` - Performance benchmarks
6. ✅ `test_performance_optimizations.py` - Optimization tests

### **Integration Tests (Optional but useful)**
7. ✅ `test_sales_count.py` - Sales count validation
8. ✅ `test_crawl_recursive.py` - Recursive crawling

---

## 📁 **Test File Descriptions**

### **test_cache_hit_rate_fix.py**
**Purpose:** Validates cache hit rate optimization fix (10% → 60-80%)

**Tests:**
- Config constants (TTL values)
- URL canonicalization (removes tracking params)
- Flexible product detail validation
- Cache key generation consistency
- Cache hit scenario simulation

**Expected Results:**
- All 5 test suites pass
- Demonstrates 6-8x cache hit rate improvement

**Run:**
```bash
python tests/test_cache_hit_rate_fix.py
```

---

### **test_utils.py**
**Purpose:** Tests utility functions used across the pipeline

**Tests:**
- `parse_sales_count()` - Extract sales count from text
- `parse_price()` - Extract price information
- `extract_product_id()` - Extract product ID from URL
- Module imports validation

**Run:**
```bash
python tests/test_utils.py
```

---

### **test_crawl_products.py**
**Purpose:** Tests product crawling functionality

**Tests:**
- Parse products from HTML
- Crawl single category
- Crawl multiple categories
- Get page with Selenium/requests

**Run:**
```bash
python tests/test_crawl_products.py
```

---

### **test_transform_load.py**
**Purpose:** Tests data transformation and loading

**Tests:**
- Transform validation
- Data normalization
- Database format conversion
- Duplicate handling
- Load to file
- Integration testing

**Run:**
```bash
python tests/test_transform_load.py
```

---

### **test_parallel_benchmark.py**
**Purpose:** Benchmarks parallel vs sequential crawling performance

**Tests:**
- Sequential crawl speed
- Parallel crawl speed (ThreadPoolExecutor)
- Parallel crawl speed (ProcessPoolExecutor)
- Performance comparisons

**Run:**
```bash
python tests/test_parallel_benchmark.py
```

---

### **test_sales_count.py**
**Purpose:** Validates sales_count field in crawled products

**Tests:**
- Sales count in product listings
- Sales count in product details
- Sales count parsing accuracy

**Run:**
```bash
python tests/test_sales_count.py
```

---

### **test_crawl_recursive.py**
**Purpose:** Tests recursive category crawling functionality

**Tests:**
- Recursive category discovery
- Category tree building
- Category URL extraction

**Run:**
```bash
python tests/test_crawl_recursive.py
```

---

## 🗑️ **Files Marked for Cleanup**

### **Deprecated Tests (Can Archive)**

Move to `tests/archive/` directory:

1. `check_sales_count_in_data.py` - One-time validation script
2. `final_validation.py` - Phase completion check (no longer needed)
3. `test_phase4_complete.py` - Specific phase validation
4. `test_phase4_loader.py` - Old loader benchmark
5. `validate_optimized_dag.py` - One-time DAG check
6. `test_quick_crawl.py` - Prototype/experimental code

**Cleanup Command:**
```bash
# Create archive directory
mkdir -p tests/archive

# Move deprecated tests
Move-Item tests/check_sales_count_in_data.py tests/archive/
Move-Item tests/final_validation.py tests/archive/
Move-Item tests/test_phase4_complete.py tests/archive/
Move-Item tests/test_phase4_loader.py tests/archive/
Move-Item tests/validate_optimized_dag.py tests/archive/
Move-Item tests/test_quick_crawl.py tests/archive/
```

---

## 🚀 **Running All Active Tests**

### **Quick Test Suite**
```bash
# Run core functionality tests
python tests/test_cache_hit_rate_fix.py
python tests/test_utils.py
python tests/test_transform_load.py
```

### **Full Test Suite**
```bash
# Run all active tests
python tests/test_cache_hit_rate_fix.py
python tests/test_utils.py
python tests/test_crawl_products.py
python tests/test_transform_load.py
python tests/test_parallel_benchmark.py
python tests/test_sales_count.py
python tests/test_crawl_recursive.py
```

### **CI/CD Integration**
```bash
# Use project CI script
.\scripts\ci.ps1 test  # Windows
make test              # Unix/Linux
```

---

## 📊 **Test Coverage**

| Component | Test File | Coverage |
|-----------|-----------|----------|
| Cache System | `test_cache_hit_rate_fix.py` | ✅ High |
| Utilities | `test_utils.py` | ✅ High |
| Crawling | `test_crawl_products.py`, `test_crawl_recursive.py` | ✅ Medium |
| Transform/Load | `test_transform_load.py` | ✅ High |
| Performance | `test_parallel_benchmark.py` | ✅ Medium |
| Data Quality | `test_sales_count.py` | ✅ Medium |

---

## 🔧 **Adding New Tests**

### **Test Naming Convention**
- Functional tests: `test_<component>.py`
- Integration tests: `test_<feature>_integration.py`
- Benchmark tests: `test_<feature>_benchmark.py`

### **Test Structure Template**
```python
"""
Test <Component Name>

Purpose: <What this test validates>
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def test_feature():
    """Test specific feature"""
    # Arrange
    # Act
    # Assert
    pass

def main():
    """Run all tests"""
    print("=" * 70)
    print("TEST: <Component Name>")
    print("=" * 70)
    
    # Run tests
    test_feature()
    
    print("\nAll tests passed!")

if __name__ == "__main__":
    main()
```

---

## 📝 **Test Maintenance Guidelines**

1. **Active Tests:**
   - Keep updated with code changes
   - Add new tests when adding features
   - Fix failing tests immediately

2. **Deprecated Tests:**
   - Move to `tests/archive/`
   - Document reason for deprecation
   - Keep for historical reference

3. **Test Documentation:**
   - Update this README when adding/removing tests
   - Include purpose and expected results
   - Provide clear run instructions

---

## 🆘 **Troubleshooting**

### **Import Errors**
```bash
# Ensure project root is in Python path
cd e:\Project\tiki-data-pipeline
python tests/<test_file>.py
```

### **Missing Dependencies**
```bash
# Install required packages
pip install -r requirements.txt
```

### **Database Connection Errors**
```bash
# Ensure PostgreSQL is running
docker-compose up -d postgres

# Or check connection in .env file
```

---

**Last Updated:** November 19, 2025  
**Maintained By:** GitHub Copilot AI Assistant
