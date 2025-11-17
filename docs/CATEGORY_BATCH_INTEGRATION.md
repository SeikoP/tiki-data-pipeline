# Category Batch Processing Integration

## Overview

Integrated batch processing mechanism into category crawling to match the optimization pattern used for product detail crawling. This reduces Airflow task overhead and enables driver pooling for better performance.

## Changes Made

### 1. Created `crawl_categories_batch.py` Module

**Location:** `src/pipelines/crawl/crawl_categories_batch.py`

**Key Functions:**
- `crawl_single_category_with_pool()`: Crawls one category with optional driver pool support
- `crawl_category_batch()`: Processes a batch of categories with driver pooling

**Features:**
- Driver pooling support (reuses Selenium drivers across categories in batch)
- Per-category error handling (one failure doesn't fail entire batch)
- Batch statistics logging
- Automatic pool initialization and cleanup

### 2. Updated Airflow DAG

**File:** `airflow/dags/tiki_crawl_products_dag.py`

#### Import Section (Lines ~280-330)
Added import for `crawl_categories_batch` module:
```python
# Import module crawl_categories_batch (for category batch processing)
crawl_categories_batch_path = None
for path in possible_paths:
    test_path = os.path.join(path, "crawl_categories_batch.py")
    if os.path.exists(test_path):
        crawl_categories_batch_path = test_path
        break

if crawl_categories_batch_path and os.path.exists(crawl_categories_batch_path):
    # ... import logic with fallback
    crawl_category_batch = crawl_categories_batch_module.crawl_category_batch
else:
    crawl_category_batch = None  # Fallback to single-category processing
```

#### Batch Preparation Function (Lines ~4800-4880)
Added `prepare_category_batch_kwargs()` function:
```python
def prepare_category_batch_kwargs(**context):
    """
    Helper function to prepare op_kwargs for Dynamic Task Mapping with batch processing
    
    Chia categories thành batches để giảm số lượng Airflow tasks và tận dụng driver pooling
    """
    # ... implementation
    batch_size = int(Variable.get("TIKI_CATEGORY_BATCH_SIZE", default_var="5"))
    # Chia categories thành batches
    # Trả về list các dict để expand (mỗi dict là 1 batch)
```

#### Task Wiring (Lines ~4880-4910)
Modified `task_crawl_category` to use batch processing:
```python
# New task for batch processing
task_prepare_batch_kwargs = PythonOperator(
    task_id="prepare_batch_kwargs",
    python_callable=prepare_category_batch_kwargs,
    execution_timeout=timedelta(minutes=1),
)

# Use batch processing if available, otherwise fallback
if crawl_category_batch is not None:
    task_crawl_category = PythonOperator.partial(
        task_id="crawl_category",
        python_callable=crawl_category_batch,
        execution_timeout=timedelta(minutes=15),  # 5 categories × 3 min
        pool="default_pool",
        retries=1,
    ).expand(op_kwargs=task_prepare_batch_kwargs.output)
else:
    # Fallback to single-category processing
    task_crawl_category = PythonOperator.partial(
        task_id="crawl_category",
        python_callable=crawl_single_category,
        execution_timeout=timedelta(minutes=10),
        pool="default_pool",
        retries=1,
    ).expand(op_kwargs=task_prepare_crawl.output)
```

#### Merge Function Update (Lines ~1340-1380)
Enhanced `merge_products()` to handle batch results:
```python
# Check if result is a list (batch result) or dict (single category result)
if isinstance(result, list):
    # Batch result - flatten it
    for single_result in result:
        if single_result and isinstance(single_result, dict):
            # Process single_result...
elif result and isinstance(result, dict):
    # Single category result
    # Process result...
```

## Configuration

### Airflow Variables

Add these variables in Airflow UI (Admin → Variables):

| Variable Name | Default | Description |
|--------------|---------|-------------|
| `TIKI_CATEGORY_BATCH_SIZE` | `5` | Number of categories per batch (5-10 recommended) |
| `TIKI_CATEGORY_POOL_SIZE` | `3` | Number of Selenium drivers in pool (3-5 recommended) |

### Recommendations

**Batch Size:**
- Small projects: 5 categories/batch
- Medium projects: 7-8 categories/batch
- Large projects: 10 categories/batch (max recommended)

**Pool Size:**
- Conservative: 3 drivers (low memory usage)
- Balanced: 5 drivers (recommended for most cases)
- Aggressive: 7-10 drivers (high memory, faster crawling)

**Calculation:**
- 1 category ≈ 2-3 minutes (depends on products per category)
- Batch timeout = batch_size × 3 minutes
- Pool size should be ≤ batch_size for efficiency

## Performance Benefits

### Before (Single Category Processing)
- **Airflow overhead:** High (1 task per category)
- **Driver reuse:** None (create/quit per category)
- **Parallelism:** Limited by Airflow task slots
- **Retry granularity:** Per category (good)

### After (Batch Processing)
- **Airflow overhead:** Low (1 task per 5-10 categories)
- **Driver reuse:** High (drivers reused within batch)
- **Parallelism:** Better (fewer tasks = more efficient scheduling)
- **Retry granularity:** Per batch, but categories retry individually

### Expected Speedup
- **Driver pooling:** 20-40% faster per category (eliminates create/quit overhead)
- **Batch processing:** 30-50% reduction in total DAG run time (reduced Airflow overhead)
- **Combined:** 50-70% overall speedup compared to original implementation

## Testing

### Local Testing
```powershell
# Test crawl_categories_batch module standalone
python src/pipelines/crawl/crawl_categories_batch.py
```

### Airflow Testing
1. Start Docker stack: `docker-compose up -d --build`
2. Access Airflow UI: http://localhost:8080
3. Set Airflow Variables (if not already set):
   - `TIKI_CATEGORY_BATCH_SIZE=5`
   - `TIKI_CATEGORY_POOL_SIZE=3`
4. Trigger DAG: `tiki_crawl_products`
5. Monitor logs for batch processing messages:
   - `📦 Đã chia thành X batches`
   - `📦 BATCH Y: Crawl Z categories`
   - `🎯 Pool size: N drivers`

### Log Indicators

**Batch Processing Active:**
```
📦 Đã chia thành 8 batches (mỗi batch 5 categories)
📦 BATCH 0: Crawl 5 categories with driver pool
🎯 Pool size: 3 drivers
✅ BATCH 0: 5/5 categories thành công
```

**Fallback to Single Category:**
```
⚠️ Không thể import crawl_categories_batch module
📋 Using single-category processing
🛍️ TASK: Crawl Category - [category name]
```

## Troubleshooting

### Batch Module Not Found
**Symptom:** Logs show "Không thể import crawl_categories_batch module"
**Solution:** 
- Verify file exists: `src/pipelines/crawl/crawl_categories_batch.py`
- Check Docker volume mounts in `docker-compose.yaml`
- Rebuild Airflow image: `docker-compose up -d --build`

### High Memory Usage
**Symptom:** Container crashes or OOM errors
**Solution:**
- Reduce `TIKI_CATEGORY_POOL_SIZE` (try 3 instead of 5)
- Reduce `TIKI_CATEGORY_BATCH_SIZE` (try 5 instead of 10)
- Increase Docker memory limit

### Batch Timeouts
**Symptom:** Task fails with "execution timeout exceeded"
**Solution:**
- Reduce batch size to crawl fewer categories per task
- Increase `execution_timeout` in DAG (currently 15 minutes)
- Check network connectivity and Tiki.vn response times

### Results Not Merging
**Symptom:** `merge_products` shows 0 products or wrong count
**Solution:**
- Check logs for XCom pull errors
- Verify batch results are properly formatted (list of dicts)
- Check merge function's batch flattening logic

## Rollback Plan

If batch processing causes issues, rollback by:

1. **Temporary:** Set `TIKI_CATEGORY_BATCH_SIZE=1` (effectively disables batching)
2. **Permanent:** Remove batch logic from DAG:
   ```python
   # Comment out batch task
   # task_prepare_batch_kwargs = ...
   
   # Restore original single-category task
   task_crawl_category = PythonOperator.partial(
       task_id="crawl_category",
       python_callable=crawl_single_category,
       # ... original config
   ).expand(op_kwargs=task_prepare_crawl.output)
   ```

## Future Enhancements

- [ ] Add batch progress tracking to resume from mid-batch failures
- [ ] Implement adaptive batch sizing based on category product counts
- [ ] Add batch-level caching to skip already-crawled batches
- [ ] Create separate Airflow pool for category crawling
- [ ] Add Prometheus metrics for batch performance monitoring

## References

- Product detail batch implementation: `airflow/dags/tiki_crawl_products_dag.py` (lines 1910-2050, 4880-4950)
- Driver pooling implementation: `src/pipelines/crawl/utils.py` (SeleniumDriverPool class)
- Optimization guide: `docs/OPTIMIZATION_GUIDE.md`
