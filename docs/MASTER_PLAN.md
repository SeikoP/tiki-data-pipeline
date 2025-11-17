# 🎯 MASTER PLAN - TIKI DATA PIPELINE OPTIMIZATION & CLEANUP

> **Created:** November 17, 2025  
> **Branch:** optimize  
> **Goal:** Tối ưu toàn diện dự án với cleanup + performance improvements

---

## 📋 TABLE OF CONTENTS

1. [Tổng Quan](#-tổng-quan)
2. [Phase 1: Project Cleanup](#phase-1-project-cleanup-completed-)
3. [Phase 2: Quick Wins Optimization](#phase-2-quick-wins-optimization)
4. [Phase 3: Code Structure Refactoring](#phase-3-code-structure-refactoring)
5. [Phase 4: Performance Optimization](#phase-4-performance-optimization)
6. [Phase 5: Infrastructure Optimization](#phase-5-infrastructure-optimization)
7. [Phase 6: Monitoring & Observability](#phase-6-monitoring--observability)
8. [Timeline & Resources](#-timeline--resources)
9. [Success Metrics](#-success-metrics)

---

## 🎯 TỔNG QUAN

### Mục Tiêu Chính
- ✅ Cleanup dự án (loại bỏ duplicate, outdated files)
- 🎯 Giảm pipeline execution time từ **6-8 giờ → 2-3 giờ** (60-66%)
- 🎯 Cải thiện code maintainability (modular DAG)
- 🎯 Tối ưu resource usage (memory, CPU)
- 🎯 Implement monitoring & alerting

### Phạm Vi
- **In Scope:** DAG refactoring, performance optimization, infrastructure tuning
- **Out of Scope:** Migration to other platforms, major architecture changes

---

## PHASE 1: PROJECT CLEANUP ✅ (COMPLETED)

### 1.1. Cache & Logs Cleanup ✅
**Status:** COMPLETED  
**Results:**
- ✅ Deleted `.mypy_cache`
- ✅ Deleted `.ruff_cache`
- ✅ Deleted `docs/.md` (empty file)
- ✅ No old logs found (< 7 days)

### 1.2. Docker Cleanup ✅
**Status:** COMPLETED  
**Results:**
- ✅ Pruned unused images: **177 MB**
- ✅ Pruned build cache: **6.82 GB**
- ✅ Pruned unused networks

**Total Space Saved:** ~7 GB

### 1.3. Code Organization ✅
**Status:** COMPLETED  
**Actions:**
- ✅ Deleted duplicate architecture docs (`architecture.drawio.xml`, `architecture.puml`)
- ✅ Moved utility files from `tests/` to `scripts/utils/`:
  - `analyze_failed_tasks.py`
  - `check_code_quality.py`
  - `check_errors.py`
- ✅ Moved `setup_airflow_variables.py` to `scripts/`
- ✅ Moved `demo_transform_output.py` to `demos/`
- ✅ Created `scripts/utils/` directory

### 1.4. Automation Script ✅
**Status:** COMPLETED  
**Created:** `scripts/cleanup.ps1`

**Features:**
```powershell
# Usage
.\scripts\cleanup.ps1 -All         # Clean everything
.\scripts\cleanup.ps1 -Cache       # Clean Python cache
.\scripts\cleanup.ps1 -Logs        # Clean old logs
.\scripts\cleanup.ps1 -Docker      # Clean Docker resources
.\scripts\cleanup.ps1 -Data        # Clean old data files
.\scripts\cleanup.ps1 -DryRun      # Preview changes
```

---

## PHASE 2: QUICK WINS OPTIMIZATION

**Timeline:** 1-2 days  
**Priority:** HIGH  
**Risk:** LOW

### 2.1. Selenium Wait Time Optimization
**File:** `src/pipelines/crawl/crawl_products_detail.py`

**Current:**
```python
driver.implicitly_wait(10)  # TOO LONG!
time.sleep(2)
```

**Optimized:**
```python
driver.implicitly_wait(3)   # Reduce to 3s
time.sleep(0.5)             # Reduce to 0.5s with explicit waits
```

**Impact:** 50-75% faster per product  
**Effort:** 2 hours

### 2.2. PostgreSQL Indexes
**File:** Create `airflow/setup/add_performance_indexes.sql`

```sql
-- Products table indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_category_url 
  ON products(category_url);
  
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_created_at 
  ON products(created_at DESC);
  
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_sales_count 
  ON products(sales_count DESC) 
  WHERE sales_count IS NOT NULL;

-- JSONB indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_products_specs_gin 
  ON products USING GIN(specifications);

-- Categories table indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_categories_parent_url 
  ON categories(parent_url);
  
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_categories_level 
  ON categories(level);
```

**Impact:** 30-50% faster queries  
**Effort:** 1 hour

### 2.3. Redis Connection Pooling
**File:** `src/pipelines/crawl/storage/redis_cache.py`

**Current:** Tạo connection mỗi lần

**Optimized:**
```python
import redis

class RedisCache:
    def __init__(self):
        self.pool = redis.ConnectionPool(
            host='redis',
            port=6379,
            db=1,
            max_connections=20,
            decode_responses=True
        )
        self.client = redis.Redis(connection_pool=self.pool)
```

**Impact:** 20-30% faster cache operations  
**Effort:** 2 hours

### 2.4. Performance Logging
**File:** Create `src/common/monitoring.py`

```python
import time
from functools import wraps
import logging

logger = logging.getLogger(__name__)

def measure_time(operation_name: str = None):
    """Decorator to measure function execution time"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            name = operation_name or func.__name__
            start = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start
                logger.info(f"⏱️ {name} completed in {duration:.2f}s")
                return result
            except Exception as e:
                duration = time.time() - start
                logger.error(f"❌ {name} failed after {duration:.2f}s: {e}")
                raise
        return wrapper
    return decorator
```

**Impact:** Better visibility into bottlenecks  
**Effort:** 3 hours

---

## PHASE 3: CODE STRUCTURE REFACTORING

**Timeline:** 1 week  
**Priority:** HIGH  
**Risk:** MEDIUM

### 3.1. Modularize DAG File (5000+ lines → modular)

**Current Structure:**
```
airflow/dags/
├── tiki_crawl_products_dag.py (5013 lines) ❌
└── tiki_crawl_products_test_dag.py (duplicate) ❌
```

**Target Structure:**
```
airflow/dags/
├── tiki_crawl_products_dag.py (200-300 lines) ✅
├── dag_config/
│   ├── __init__.py
│   └── config.py                    # DAG configuration
├── dag_tasks/
│   ├── __init__.py
│   ├── category_tasks.py            # Category crawling tasks
│   ├── product_tasks.py             # Product crawling tasks
│   ├── transform_tasks.py           # Transform tasks
│   ├── load_tasks.py                # Load tasks
│   └── validation_tasks.py          # Validation tasks
└── dag_helpers/
    ├── __init__.py
    ├── file_operations.py           # File I/O helpers
    ├── xcom_helpers.py              # XCom utilities
    └── selenium_pool.py             # Selenium driver pool
```

**Implementation Steps:**

1. **Create directory structure** (1 hour)
   ```powershell
   New-Item -ItemType Directory -Path "airflow/dags/dag_config"
   New-Item -ItemType Directory -Path "airflow/dags/dag_tasks"
   New-Item -ItemType Directory -Path "airflow/dags/dag_helpers"
   ```

2. **Extract configuration** (2 hours)
   - Move all `Variable.get()` calls to `dag_config/config.py`
   - Create typed config class

3. **Extract task functions** (1 day)
   - Move category tasks to `dag_tasks/category_tasks.py`
   - Move product tasks to `dag_tasks/product_tasks.py`
   - Move transform tasks to `dag_tasks/transform_tasks.py`
   - Move load tasks to `dag_tasks/load_tasks.py`

4. **Extract helpers** (1 day)
   - Move file operations to `dag_helpers/file_operations.py`
   - Move XCom logic to `dag_helpers/xcom_helpers.py`
   - Move Selenium pool to `dag_helpers/selenium_pool.py`

5. **Update main DAG** (1 day)
   - Import from new modules
   - Keep only DAG definition and task dependencies
   - Test thoroughly

6. **Consolidate test DAG** (2 hours)
   - Use environment variable to switch modes
   - Delete `tiki_crawl_products_test_dag.py`

**Impact:** Modular, maintainable code  
**Effort:** 5 days

### 3.2. Consolidate Duplicate DAGs

**Current:**
- `tiki_crawl_products_dag.py` (production)
- `tiki_crawl_products_test_dag.py` (test)

**Solution:**
```python
# dag_config/config.py
import os

class DAGConfig:
    ENV = os.getenv("PIPELINE_ENV", "production")
    
    if ENV == "test":
        MAX_PRODUCTS_PER_CATEGORY = 10
        MAX_CATEGORIES = 5
        SCHEDULE_INTERVAL = "@daily"
    else:
        MAX_PRODUCTS_PER_CATEGORY = 500
        MAX_CATEGORIES = None  # All categories
        SCHEDULE_INTERVAL = "@weekly"
```

**Impact:** Single source of truth  
**Effort:** 4 hours

---

## PHASE 4: PERFORMANCE OPTIMIZATION

**Timeline:** 1 week  
**Priority:** MEDIUM  
**Risk:** MEDIUM

### 4.1. Parallel Selenium Drivers

**File:** `src/pipelines/crawl/crawl_products_detail.py`

**Current:** Sequential crawling

**Optimized:**
```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def crawl_products_parallel(urls: list, max_workers: int = 4):
    """Crawl products in parallel using ThreadPoolExecutor"""
    results = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(crawl_product_detail, url): url 
            for url in urls
        }
        
        for future in as_completed(future_to_url):
            url = future_to_url[future]
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logger.error(f"Failed to crawl {url}: {e}")
    
    return results
```

**Impact:** 2-4x faster crawling  
**Effort:** 2 days

### 4.2. Database Connection Pooling

**File:** `src/pipelines/crawl/storage/postgres_storage.py`

**Current:** Create connection per operation

**Optimized:**
```python
from psycopg2 import pool
from contextlib import contextmanager

class PostgresStorage:
    def __init__(self):
        self.connection_pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", 5432)),
            database=os.getenv("POSTGRES_DB", "crawl_data"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )
    
    @contextmanager
    def get_connection(self):
        """Context manager for connection pooling"""
        conn = self.connection_pool.getconn()
        try:
            yield conn
        finally:
            self.connection_pool.putconn(conn)
    
    def query(self, sql, params=None):
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
```

**Impact:** 30-50% faster database operations  
**Effort:** 1 day

### 4.3. Batch Processing Optimization

**Transform:**
```python
def transform_products_batch(products: list, batch_size: int = 100):
    """Process products in batches for memory efficiency"""
    for i in range(0, len(products), batch_size):
        batch = products[i:i+batch_size]
        yield transform_batch(batch)
```

**Load:**
```python
def bulk_load_with_copy(products: list):
    """Use PostgreSQL COPY for bulk inserts"""
    from io import StringIO
    import csv
    
    csv_buffer = StringIO()
    writer = csv.DictWriter(csv_buffer, fieldnames=products[0].keys())
    writer.writeheader()
    writer.writerows(products)
    
    csv_buffer.seek(0)
    with self.get_connection() as conn:
        with conn.cursor() as cur:
            cur.copy_expert(
                f"COPY products FROM STDIN WITH CSV HEADER",
                csv_buffer
            )
            conn.commit()
```

**Impact:** 5-10x faster bulk operations  
**Effort:** 2 days

### 4.4. Headless Chrome Optimization

**File:** `src/pipelines/crawl/utils.py`

```python
def setup_chrome_options(headless: bool = True, disable_images: bool = True):
    """Optimized Chrome options for faster crawling"""
    options = webdriver.ChromeOptions()
    
    if headless:
        options.add_argument('--headless=new')  # New headless mode
    
    # Performance optimizations
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    
    if disable_images:
        prefs = {"profile.managed_default_content_settings.images": 2}
        options.add_experimental_option("prefs", prefs)
    
    # Memory optimizations
    options.add_argument('--disable-application-cache')
    options.add_argument('--disk-cache-size=1')
    
    return options
```

**Impact:** 20-30% faster page loads  
**Effort:** 1 day

---

## PHASE 5: INFRASTRUCTURE OPTIMIZATION

**Timeline:** 1-2 weeks  
**Priority:** MEDIUM  
**Risk:** HIGH

### 5.1. Docker Resource Allocation

**File:** `docker-compose.yaml`

```yaml
x-airflow-common:
  &airflow-common
  # ... existing config ...
  deploy:
    resources:
      limits:
        cpus: '2.0'
        memory: 4G
      reservations:
        cpus: '1.0'
        memory: 2G

services:
  postgres:
    # ... existing config ...
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 2G
        reservations:
          cpus: '0.5'
          memory: 1G
    command: >
      postgres
      -c shared_buffers=512MB
      -c effective_cache_size=1536MB
      -c maintenance_work_mem=128MB
      -c max_connections=100
      -c checkpoint_completion_target=0.9
      -c wal_buffers=16MB
      -c default_statistics_target=100

  redis:
    # ... existing config ...
    deploy:
      resources:
        limits:
          cpus: '0.5'
          memory: 512M
        reservations:
          cpus: '0.25'
          memory: 256M
    command: >
      redis-server
      --maxmemory 256mb
      --maxmemory-policy allkeys-lru
```

**Impact:** Better resource utilization  
**Effort:** 1 day

### 5.2. PostgreSQL Configuration Tuning

**File:** Create `airflow/setup/postgres_tuning.sql`

```sql
-- Memory settings
ALTER SYSTEM SET shared_buffers = '512MB';
ALTER SYSTEM SET effective_cache_size = '1536MB';
ALTER SYSTEM SET maintenance_work_mem = '128MB';
ALTER SYSTEM SET work_mem = '16MB';

-- WAL settings
ALTER SYSTEM SET wal_buffers = '16MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
ALTER SYSTEM SET min_wal_size = '1GB';
ALTER SYSTEM SET max_wal_size = '4GB';

-- Query planner
ALTER SYSTEM SET random_page_cost = 1.1;
ALTER SYSTEM SET effective_io_concurrency = 200;

-- Monitoring
ALTER SYSTEM SET log_min_duration_statement = '1000';  -- Log queries > 1s
ALTER SYSTEM SET log_line_prefix = '%t [%p]: [%l-1] user=%u,db=%d,app=%a,client=%h ';

-- Reload configuration
SELECT pg_reload_conf();
```

**Impact:** 20-40% faster queries  
**Effort:** 1 day

### 5.3. Airflow Task Concurrency

**File:** `airflow/dags/tiki_crawl_products_dag.py`

```python
default_args = {
    # ... existing args ...
    "max_active_tasks": 8,      # Increase from default
    "max_active_runs": 2,        # Allow 2 parallel DAG runs
    "pool": "crawl_pool",        # Use dedicated pool
}

# Create pool in Airflow UI or via script:
# Pool name: crawl_pool
# Slots: 16
```

**Impact:** Better parallelism  
**Effort:** 2 hours

---

## PHASE 6: MONITORING & OBSERVABILITY

**Timeline:** 1 week  
**Priority:** LOW  
**Risk:** LOW

### 6.1. Performance Metrics Collection

**File:** Create `src/common/metrics.py`

```python
import time
from dataclasses import dataclass, asdict
from typing import Optional
import json
from datetime import datetime

@dataclass
class PerformanceMetrics:
    """Performance metrics for pipeline operations"""
    operation: str
    start_time: datetime
    end_time: datetime
    duration_seconds: float
    success: bool
    items_processed: int
    error_message: Optional[str] = None
    
    def to_dict(self):
        return asdict(self)
    
    def to_json(self):
        return json.dumps(self.to_dict(), default=str)

class MetricsCollector:
    """Collect and store performance metrics"""
    
    def __init__(self, output_file: str = "metrics/pipeline_metrics.jsonl"):
        self.output_file = output_file
    
    def record_metric(self, metric: PerformanceMetrics):
        """Record a performance metric"""
        with open(self.output_file, 'a') as f:
            f.write(metric.to_json() + '\n')
```

**Usage:**
```python
collector = MetricsCollector()

start = datetime.now()
try:
    # ... operation ...
    collector.record_metric(PerformanceMetrics(
        operation="crawl_products",
        start_time=start,
        end_time=datetime.now(),
        duration_seconds=(datetime.now() - start).total_seconds(),
        success=True,
        items_processed=len(products)
    ))
except Exception as e:
    collector.record_metric(PerformanceMetrics(
        operation="crawl_products",
        start_time=start,
        end_time=datetime.now(),
        duration_seconds=(datetime.now() - start).total_seconds(),
        success=False,
        items_processed=0,
        error_message=str(e)
    ))
```

**Impact:** Data-driven optimization decisions  
**Effort:** 2 days

### 6.2. DAG Performance Dashboard

**File:** Create `scripts/generate_performance_report.py`

```python
#!/usr/bin/env python3
"""Generate performance report from metrics"""

import json
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

def generate_report(metrics_file: str):
    """Generate performance report with charts"""
    
    # Load metrics
    metrics = []
    with open(metrics_file) as f:
        for line in f:
            metrics.append(json.loads(line))
    
    df = pd.DataFrame(metrics)
    
    # Generate charts
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # 1. Duration by operation
    df.groupby('operation')['duration_seconds'].mean().plot(
        kind='bar', ax=axes[0, 0], title='Average Duration by Operation'
    )
    
    # 2. Success rate
    success_rate = df.groupby('operation')['success'].mean() * 100
    success_rate.plot(
        kind='bar', ax=axes[0, 1], title='Success Rate (%)'
    )
    
    # 3. Items processed over time
    df['start_time'] = pd.to_datetime(df['start_time'])
    df.set_index('start_time')['items_processed'].plot(
        ax=axes[1, 0], title='Items Processed Over Time'
    )
    
    # 4. Duration trend
    df.set_index('start_time')['duration_seconds'].rolling(10).mean().plot(
        ax=axes[1, 1], title='Duration Trend (10-run moving average)'
    )
    
    plt.tight_layout()
    plt.savefig('metrics/performance_report.png')
    
    print(f"✅ Report saved to metrics/performance_report.png")
    print("\n📊 Summary Statistics:")
    print(df.groupby('operation').agg({
        'duration_seconds': ['mean', 'min', 'max'],
        'success': 'mean',
        'items_processed': 'sum'
    }))

if __name__ == "__main__":
    generate_report("metrics/pipeline_metrics.jsonl")
```

**Impact:** Visual insights into performance  
**Effort:** 1 day

---

## 📅 TIMELINE & RESOURCES

### Timeline Overview

```
Week 1: Phase 1 (Cleanup) + Phase 2 (Quick Wins)
Week 2-3: Phase 3 (Code Refactoring)
Week 4-5: Phase 4 (Performance Optimization)
Week 6-7: Phase 5 (Infrastructure Optimization)
Week 8: Phase 6 (Monitoring)
Week 9: Testing & Validation
Week 10: Production Deployment
```

### Resource Requirements

| Phase | Developer Time | Testing Time | Total |
|-------|---------------|--------------|-------|
| Phase 1 | 1 day | 0.5 day | 1.5 days |
| Phase 2 | 2 days | 1 day | 3 days |
| Phase 3 | 5 days | 2 days | 7 days |
| Phase 4 | 5 days | 3 days | 8 days |
| Phase 5 | 7 days | 3 days | 10 days |
| Phase 6 | 3 days | 1 day | 4 days |
| **Total** | **23 days** | **10.5 days** | **33.5 days (~7 weeks)** |

---

## 📈 SUCCESS METRICS

### Performance Metrics

| Metric | Baseline | Target | Measurement |
|--------|----------|--------|-------------|
| **Full Pipeline Time** | 6-8 hours | 2-3 hours | Airflow DAG duration |
| **Crawl Time/Product** | 2s | 0.5-1s | Average per product |
| **Transform Time** | 5 min | 1-2 min | Task duration |
| **Load Time** | 10 min | 2-3 min | Task duration |
| **Memory Usage (Airflow)** | 4GB | 2-3GB | Docker stats |
| **CPU Usage (Airflow)** | 70-80% | 40-50% | Docker stats |
| **Database Query Time (avg)** | 200ms | 80-100ms | PostgreSQL logs |

### Code Quality Metrics

| Metric | Baseline | Target |
|--------|----------|--------|
| **DAG File Lines** | 5013 | < 300 |
| **Code Duplication** | 2 DAG files | 1 DAG file |
| **Test Coverage** | N/A | > 70% |
| **Linting Errors** | Check needed | 0 |
| **Type Coverage** | Low | > 80% |

### Reliability Metrics

| Metric | Target |
|--------|--------|
| **DAG Success Rate** | > 95% |
| **Task Retry Rate** | < 5% |
| **Error Recovery Time** | < 30 min |
| **Uptime** | > 99% |

---

## 🚀 GETTING STARTED

### For Phase 2 (Quick Wins):
```powershell
# 1. Run cleanup script
.\scripts\cleanup.ps1 -All

# 2. Apply database indexes
psql -U $POSTGRES_USER -d crawl_data -f airflow/setup/add_performance_indexes.sql

# 3. Update Selenium wait times
# Edit src/pipelines/crawl/crawl_products_detail.py

# 4. Test changes
.\scripts\ci.ps1 test
```

### For Phase 3 (Refactoring):
```powershell
# 1. Create branch
git checkout -b feature/dag-refactoring

# 2. Create directory structure
# (See Phase 3 implementation steps)

# 3. Gradually move code to modules
# Test after each module extraction

# 4. Update imports in main DAG
# Validate DAG: .\scripts\ci.ps1 validate-dags
```

---

## 📝 NOTES

- **Branch Strategy:** Create feature branches for each phase
- **Testing:** Test each phase thoroughly before moving to next
- **Rollback Plan:** Keep old code commented for quick rollback
- **Documentation:** Update docs as changes are implemented
- **Team Communication:** Daily standups during refactoring phases

---

## ✅ CHECKLIST

### Phase 1: Cleanup ✅
- [x] Delete Python cache folders
- [x] Delete old Airflow logs
- [x] Clean Docker resources
- [x] Remove duplicate documentation files
- [x] Reorganize test utilities
- [x] Create cleanup automation script

### Phase 2: Quick Wins
- [ ] Reduce Selenium wait times
- [ ] Add PostgreSQL indexes
- [ ] Implement Redis connection pooling
- [ ] Add performance logging decorators
- [ ] Test and validate improvements

### Phase 3: Code Refactoring
- [ ] Create modular directory structure
- [ ] Extract configuration to separate module
- [ ] Extract category tasks
- [ ] Extract product tasks
- [ ] Extract transform tasks
- [ ] Extract load tasks
- [ ] Extract helper utilities
- [ ] Update main DAG file
- [ ] Consolidate test DAG
- [ ] Update tests and documentation

### Phase 4: Performance Optimization
- [ ] Implement parallel Selenium drivers
- [ ] Add database connection pooling
- [ ] Optimize batch processing
- [ ] Optimize Chrome options
- [ ] Test and benchmark improvements

### Phase 5: Infrastructure Optimization
- [ ] Configure Docker resource limits
- [ ] Tune PostgreSQL configuration
- [ ] Optimize Airflow task concurrency
- [ ] Test infrastructure changes

### Phase 6: Monitoring
- [ ] Implement metrics collection
- [ ] Create performance dashboard
- [ ] Set up alerting (optional)
- [ ] Document monitoring procedures

---

**Last Updated:** November 17, 2025  
**Status:** Phase 1 Complete ✅ | Phase 2 Ready to Start 🚀
