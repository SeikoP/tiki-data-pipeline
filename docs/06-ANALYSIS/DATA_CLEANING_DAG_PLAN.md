# Kế hoạch DAG Airflow thứ 2: Làm sạch dữ liệu toàn diện

## Tổng quan
DAG này tập trung vào việc làm sạch, chuẩn hóa và làm giàu dữ liệu sản phẩm đã crawl, tách biệt khỏi luồng crawl chính để:
- Tối ưu tài nguyên (chạy độc lập, có thể schedule khác)
- Dễ debug và maintain
- Cho phép re-run cleaning mà không cần crawl lại
- Áp dụng các quy tắc cleaning phức tạp hơn

## Mục tiêu
1. **Làm sạch dữ liệu thô**: Loại bỏ duplicate, invalid, incomplete records
2. **Chuẩn hóa**: Đồng nhất format, units, encoding
3. **Làm giàu dữ liệu**: Thêm computed fields, category mapping, brand normalization
4. **Phát hiện anomaly**: Giá bất thường, sales count không hợp lý
5. **Tạo dataset sạch**: Output cho analytics và reporting

## Kiến trúc DAG

### Input (Khuyến nghị: Từ Database)
**Option 1: Database (Recommended)** ✅
- Source: PostgreSQL `crawl_data.products` table (từ DAG crawl)
- Metadata: PostgreSQL `crawl_data.categories` table
- Config: Airflow Variables cho thresholds

**Lý do chọn Database:**
- ✅ **Hiệu năng**: Query trực tiếp nhanh hơn load toàn bộ JSON files
- ✅ **Filtering**: SQL WHERE để lọc data cần clean (theo date, status)
- ✅ **Incremental**: Chỉ xử lý records mới/thay đổi (WHERE processed_at IS NULL)
- ✅ **Scalability**: Handle millions records dễ dàng
- ✅ **Atomicity**: Transaction support, rollback nếu fail
- ✅ **Joining**: Dễ dàng join với categories, sellers, etc.
- ✅ **No I/O bottleneck**: Không cần parse JSON files lớn

**Option 2: File-based (Fallback)**
- Source: `data/raw/products/*.json` (từ DAG crawl)
- Metadata: `data/raw/categories.json`
- Use case: Local development, testing, hoặc khi DB không available

### Output (Hybrid: Database + File)
**Database (Primary)** ✅
- Clean data: PostgreSQL `crawl_data.products` table (update `is_clean = true, cleaned_at = now()`)
- Quality flags: Add columns `quality_score`, `has_anomaly`, `anomaly_flags` JSONB
- Audit trail: `crawl_data.cleaning_audit` table (log mọi transformation)

**Files (Secondary - cho export/backup)**
- Clean data export: `data/processed/products_clean.json` (daily snapshot)
- Rejected data: `data/processed/products_rejected.json` (để review)
- Stats report: `data/processed/cleaning_stats.json`
- Quality metrics: `data/processed/quality_metrics.json`

## Các TaskGroup chính

### 1️⃣ TaskGroup: Data Validation & Filtering
**Mục đích**: Lọc ra các records hợp lệ, loại bỏ garbage data

#### Task 1.1: `load_raw_products`
**Database approach** (recommended):
```python
# Query chỉ products chưa clean hoặc cần re-clean
query = """
    SELECT * FROM crawl_data.products 
    WHERE (is_clean = FALSE OR is_clean IS NULL)
    AND crawled_at >= NOW() - INTERVAL '7 days'
    ORDER BY crawled_at DESC
"""
# Hoặc load theo batch để tránh OOM
batch_size = 10000
offset = 0
```

**File approach** (fallback):
- Load tất cả JSON files từ `data/raw/products/`
- Merge thành một dataset thống nhất

**Output**: DataFrame hoặc XCom với product count

#### Task 1.2: `validate_required_fields`
- Kiểm tra required fields: `product_id`, `name`, `url`
- Validate format: `product_id` phải là số, `url` phải hợp lệ
- Reject records thiếu fields quan trọng
- **Threshold**: Airflow Variable `min_required_fields_completion_rate` (default: 95%)

#### Task 1.3: `remove_duplicates`
- Phát hiện duplicate dựa trên `product_id`
- Strategy: Giữ record mới nhất (theo `crawled_at`)
- Log số lượng duplicates removed
- **Metric**: Duplicate rate

#### Task 1.4: `filter_test_data`
- Loại bỏ test products (tên chứa "test", "dummy", "sample")
- Loại bỏ products có giá = 0 hoặc quá cao (> 1 tỷ)
- Loại bỏ products không có category_url

#### Task 1.5: `check_data_completeness`
- Tính completeness score cho mỗi product (% fields có giá trị)
- Filter products có completeness < threshold
- **Threshold**: Airflow Variable `min_completeness_score` (default: 60%)

**Output**: `validated_products.json` (XCom)

---

### 2️⃣ TaskGroup: Data Normalization
**Mục đích**: Chuẩn hóa format, encoding, units

#### Task 2.1: `normalize_text_fields`
- Chuẩn hóa `name`: Trim, remove extra spaces, title case
- Chuẩn hóa `brand`: Remove prefix "Thương hiệu:", standardize spelling
- Chuẩn hóa `seller_name`: Trim, remove special chars
- **Unicode normalization**: NFD → NFC (Vietnamese chars)

#### Task 2.2: `normalize_numeric_fields`
- Parse `sales_count` từ text (xử lý "1k", "1.2tr", etc.)
- Chuẩn hóa giá: Loại bỏ currency symbols, convert về VND
- Round `rating_average` về 1 decimal place
- Ensure `discount_percent` trong khoảng [0, 100]

#### Task 2.3: `normalize_urls`
- Chuẩn hóa `url`: Remove tracking params, force https
- Chuẩn hóa `category_url`: Consistent format
- Extract `category_id` từ URL nếu chưa có
- Validate URL accessibility (optional, có thể bật/tắt)

#### Task 2.4: `standardize_dates`
- Parse `crawled_at` về ISO-8601 format
- Add `processed_at` timestamp
- Calculate `data_age` (hours since crawl)

#### Task 2.5: `normalize_seller_info`
- Standardize `seller_is_official` về boolean
- Map seller names to canonical form (xử lý typos)
- Extract `seller_type` (official, marketplace, individual)

**Output**: `normalized_products.json` (XCom)

---

### 3️⃣ TaskGroup: Data Enrichment
**Mục đích**: Thêm computed fields và metadata

#### Task 3.1: `compute_revenue_metrics`
- `estimated_revenue` = `sales_count * price`
- `price_savings` = `original_price - price`
- `discount_amount` = same as `price_savings`
- `revenue_per_review` = `estimated_revenue / review_count`

#### Task 3.2: `compute_popularity_metrics`
- `popularity_score` (0-100): Weighted combination
  - Sales count: 50%
  - Rating: 30%
  - Review count: 20%
- `engagement_rate` = `review_count / sales_count * 100`
- `sales_velocity` = `sales_count` (hoặc rate nếu có historical data)

#### Task 3.3: `compute_value_metrics`
- `value_score` = `rating_average / (price / 1_000_000)`
- `price_per_rating_point` = `price / rating_average`
- `quality_price_ratio` = `(rating_average / 5) / (price / max_price)`

#### Task 3.4: `classify_products`
- `price_category`: budget | mid-range | premium | luxury
- `popularity_tier`: low | medium | high | top
- `discount_tier`: no-discount | small | medium | large | flash-sale
- `review_tier`: few | some | many | viral

#### Task 3.5: `add_category_metadata`
- Join với category tree từ `data/raw/categories.json`
- Add `category_name`, `category_level`, `parent_category`
- Add `category_path` (breadcrumb)
- Compute `category_avg_price` để so sánh

**Output**: `enriched_products.json` (XCom)

---

### 4️⃣ TaskGroup: Anomaly Detection
**Mục đích**: Phát hiện dữ liệu bất thường để review

#### Task 4.1: `detect_price_anomalies`
- Phát hiện giá outliers (z-score method)
- Phát hiện `price > original_price`
- Phát hiện `discount_percent` không match với giá thực tế
- **Flag**: `has_price_anomaly` = true/false

#### Task 4.2: `detect_rating_anomalies`
- Phát hiện `rating_average` quá cao với `review_count` thấp (suspicious)
- Phát hiện `review_count` quá cao cho `sales_count` thấp
- **Threshold**: Airflow Variable `suspicious_review_ratio` (default: 0.5)

#### Task 4.3: `detect_sales_anomalies`
- Phát hiện `sales_count` quá cao so với category average (potential fraud)
- Phát hiện `sales_count = 0` với `review_count > 0` (inconsistent)
- Cross-check với `estimated_revenue` outliers

#### Task 4.4: `detect_data_quality_issues`
- Phát hiện missing critical fields (price, brand, seller)
- Phát hiện text fields quá ngắn/dài (name < 10 chars, > 500 chars)
- Phát hiện suspicious patterns (spam, fake)

#### Task 4.5: `flag_for_review`
- Aggregate tất cả anomaly flags
- Tính `quality_score` (0-100) cho mỗi product
- Tag products cần manual review
- **Output**: `products_for_review.json`

**Output**: `products_with_flags.json` (XCom)

---

### 5️⃣ TaskGroup: Data Segmentation
**Mục đích**: Phân loại và tạo subsets

#### Task 5.1: `segment_by_price`
- Split thành 4 files: budget, mid-range, premium, luxury
- Compute segment statistics

#### Task 5.2: `segment_by_category`
- Group products theo category_id
- Compute per-category metrics (avg price, sales, rating)
- Identify top categories

#### Task 5.3: `segment_by_performance`
- **Best sellers**: Top 10% sales_count
- **Top rated**: rating_average >= 4.5 và review_count >= 100
- **Best value**: Top 20% value_score
- **Trending**: High sales_velocity

#### Task 5.4: `create_recommendation_segments`
- **Popular in category**: Top 20 per category
- **Similar price range**: +/- 20% price
- **Complementary products**: Based on category path

**Output**: Multiple segment files trong `data/processed/segments/`

---

### 6️⃣ TaskGroup: Quality Assurance
**Mục đích**: Validate dữ liệu cuối cùng

#### Task 6.1: `validate_data_schema`
- Kiểm tra tất cả products có đủ required fields
- Validate data types (number, string, boolean, date)
- Ensure no null values ở critical fields

#### Task 6.2: `validate_business_rules`
- `price <= original_price`
- `discount_percent` match với giá
- `rating_average` trong [0, 5]
- `sales_count >= 0`

#### Task 6.3: `compute_quality_metrics`
- **Completeness**: % fields có giá trị
- **Accuracy**: % pass validation rules
- **Consistency**: % no anomalies
- **Timeliness**: Avg data_age
- **Overall quality score**: Weighted average

#### Task 6.4: `check_quality_thresholds`
- Compare với minimum thresholds từ Airflow Variables
- **Alert** nếu quality < threshold
- **Fail DAG** nếu critical quality issues

**Output**: `quality_metrics.json`

---

### 7️⃣ TaskGroup: Output & Reporting
**Mục đích**: Save clean data và tạo reports

#### Task 7.1: `save_clean_products`
**Database approach** (recommended):
```python
# Update products table với cleaned data và flags
UPDATE crawl_data.products SET
    is_clean = TRUE,
    cleaned_at = NOW(),
    quality_score = %s,
    has_anomaly = %s,
    anomaly_flags = %s::jsonb,
    price_category = %s,
    popularity_score = %s,
    value_score = %s
WHERE product_id = %s
```

**File export** (backup/archive):
- Daily snapshot: `data/processed/products_clean_YYYYMMDD.json`
- Compressed: `data/processed/products_clean_YYYYMMDD.json.gz`
- SQLite export (optional): `data/processed/products_YYYYMMDD.db`

#### Task 7.2: `save_rejected_products`
**Database approach**:
```python
# Insert vào rejected_products table để audit
INSERT INTO crawl_data.rejected_products 
(product_id, rejection_reason, rejection_details, rejected_at)
VALUES (%s, %s, %s::jsonb, NOW())
```

**File export** (để manual review):
- Save rejected records: `data/processed/products_rejected_YYYYMMDD.json`
- Include validation errors, anomaly flags
- Group by rejection reason

#### Task 7.3: `generate_cleaning_stats`
**Database approach**:
```python
# Query aggregated stats từ DB
stats = {
    'total_products': db.query("SELECT COUNT(*) FROM products").scalar(),
    'clean_products': db.query("SELECT COUNT(*) FROM products WHERE is_clean = TRUE").scalar(),
    'rejected_products': db.query("SELECT COUNT(*) FROM rejected_products WHERE rejected_at >= TODAY()").scalar(),
    'avg_quality_score': db.query("SELECT AVG(quality_score) FROM products WHERE is_clean = TRUE").scalar(),
    'anomaly_count': db.query("SELECT COUNT(*) FROM products WHERE has_anomaly = TRUE").scalar(),
}
```

**Stats saved to**:
- Database: `crawl_data.cleaning_stats` table (time-series)
- File: `data/processed/cleaning_stats_YYYYMMDD.json`

#### Task 7.4: `generate_html_report`
- Beautiful HTML dashboard với:
  - Summary statistics
  - Quality metrics charts
  - Top products tables
  - Anomaly highlights
  - Cleaning pipeline visualization
- Save to: `data/processed/cleaning_report.html`

#### Task 7.5: `send_notification`
- Gửi Slack/Discord notification với summary
- Include quality score, total products, alerts
- Link tới HTML report

**Output**: Final clean dataset + reports

---

## DAG Configuration

### Schedule
```python
schedule_interval = "0 2 * * *"  # 2 AM daily, sau khi crawl DAG xong
```

### Dependencies với DAG crawl
```python
# External task sensor để đợi crawl DAG hoàn tất
wait_for_crawl = ExternalTaskSensor(
    task_id='wait_for_crawl_dag',
    external_dag_id='tiki_crawl_products',
    external_task_id='final_summary',
    mode='poke',
    timeout=3600,
)
```

### Airflow Variables
```python
{
    # Validation thresholds
    "min_required_fields_completion_rate": 0.95,
    "min_completeness_score": 0.6,
    
    # Anomaly detection
    "suspicious_review_ratio": 0.5,
    "price_outlier_zscore": 3.0,
    "sales_outlier_zscore": 3.0,
    
    # Quality thresholds
    "min_overall_quality_score": 0.8,
    "min_accuracy_score": 0.95,
    
    # Processing
    "batch_size": 1000,
    "max_workers": 4,
    
    # Output
    "enable_segmentation": True,
    "enable_html_report": True,
    "send_notifications": True,
}
```

### Resource allocation
```python
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}
```

---

## Data Flow Diagram

```
┌─────────────────────────────────────────┐
│  PostgreSQL: crawl_data.products        │
│  (FROM crawl DAG)                       │
│  WHERE is_clean = FALSE                 │
└─────────────┬───────────────────────────┘
              │ SQL Query (batch)
              ▼
┌─────────────────────────────────────────┐
│  Validation & Filtering (in-memory)     │◄─── Remove: duplicates, invalid, incomplete
└─────────────┬───────────────────────────┘
              │ DataFrame
              ▼
┌─────────────────────────────────────────┐
│  Normalization (pandas/SQL UDF)         │◄─── Standardize: text, numbers, URLs, dates
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│  Enrichment (compute in Python)         │◄─── Add: computed fields, classifications
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│  Anomaly Detection (statistical)        │◄─── Flag: price, rating, sales anomalies
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│  Quality Assurance (validation)         │◄─── Validate: schema, rules, thresholds
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│  Write Back to Database (batch UPDATE)  │
│  + Insert to audit tables               │
└─────────────┬───────────────────────────┘
              │
              ├─► UPDATE products SET is_clean=TRUE, quality_score=X
              ├─► INSERT INTO rejected_products (failed records)
              ├─► INSERT INTO cleaning_audit (transformation log)
              ├─► INSERT INTO cleaning_stats (metrics)
              │
              └─► Optional exports:
                  ├─► products_clean_YYYYMMDD.json (snapshot)
                  ├─► products_rejected_YYYYMMDD.json
                  ├─► cleaning_report_YYYYMMDD.html
                  └─► segments/ (derived views)
```

---

## Monitoring & Alerting

### Key Metrics to Track
- **Processing rate**: Products/second
- **Rejection rate**: % rejected products
- **Quality score**: Overall và per-dimension
- **Anomaly rate**: % products flagged
- **Data completeness**: % fields populated
- **Pipeline duration**: Total execution time

### Alert Conditions
- Quality score < 80%
- Rejection rate > 20%
- Processing failure/timeout
- Critical validation errors
- Suspicious anomaly spike

### Logging Strategy
- Task-level logs: Details cho mỗi transformation
- Summary logs: Aggregated stats sau mỗi TaskGroup
- Error logs: Validation failures, exceptions
- Audit logs: Data lineage, transformations applied

---

## Testing Strategy

### Unit Tests
- Test functions cho normalization logic
- Test anomaly detection algorithms
- Test quality metrics computation

### Integration Tests
- Test full pipeline với sample data
- Verify output schema
- Check data integrity

### Validation Tests
- Compare input vs output counts
- Verify no data loss (except intentional rejections)
- Check computed fields accuracy

---

## Performance Optimization

### Database-First Strategy ✅
**Batch Processing**:
```python
# Xử lý theo batch để tránh OOM
BATCH_SIZE = 10000
for offset in range(0, total_count, BATCH_SIZE):
    batch = db.query(f"SELECT * FROM products WHERE ... LIMIT {BATCH_SIZE} OFFSET {offset}")
    cleaned_batch = clean_pipeline(batch)
    db.bulk_update(cleaned_batch)
```

**Parallel Processing với Database**:
- Use connection pooling (SQLAlchemy pool_size=20)
- Dynamic Task Mapping: Chia products theo category_id
- Mỗi task xử lý một category độc lập (parallel)
- Write back concurrent (với proper locking)

**Indexing cho performance**:
```sql
-- Indexes cần thiết
CREATE INDEX idx_products_is_clean ON products(is_clean);
CREATE INDEX idx_products_crawled_at ON products(crawled_at);
CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_quality_score ON products(quality_score);
```

### Caching Strategy
- Cache category metadata trong memory (nhỏ, dùng lại nhiều)
- Cache computed statistics (avg_price per category)
- Redis cache cho intermediate results (nếu cần)

### Incremental Processing ✅
**Track processed records**:
```sql
-- Chỉ xử lý records mới hoặc cần re-clean
SELECT * FROM products 
WHERE (is_clean = FALSE OR cleaned_at < crawled_at)
AND crawled_at >= NOW() - INTERVAL '24 hours'
```

**Skip unchanged products**:
- Compare hash của raw data
- Nếu không thay đổi → skip cleaning

---

## Future Enhancements

### Phase 2
- ML-based anomaly detection
- Automated brand/category mapping
- Price trend analysis
- Product similarity scoring

### Phase 3
- Real-time cleaning (streaming)
- Advanced NLP cho text normalization
- Image quality validation
- Cross-source deduplication

### Phase 4
- Data versioning (DVC)
- A/B testing cho cleaning rules
- Auto-tuning thresholds
- Predictive quality scoring

---

## File Structure
```
airflow/dags/
  └── tiki_data_cleaning_dag.py          # Main DAG file

src/pipelines/clean/
  ├── __init__.py
  ├── db_connector.py                    # PostgreSQL connection pool
  ├── validator.py                       # Validation logic
  ├── normalizer.py                      # Normalization functions
  ├── enricher.py                        # Enrichment logic
  ├── anomaly_detector.py                # Anomaly detection
  ├── quality_checker.py                 # Quality assurance
  └── reporter.py                        # Report generation

data/processed/                           # File exports (backup)
  ├── products_clean_YYYYMMDD.json       # Daily snapshot
  ├── products_clean_YYYYMMDD.json.gz   # Compressed
  ├── products_rejected_YYYYMMDD.json   # Rejected records
  ├── quality_metrics_YYYYMMDD.json     # Quality stats
  ├── cleaning_stats_YYYYMMDD.json      # Processing stats
  └── cleaning_report_YYYYMMDD.html     # Visual report

PostgreSQL Schema:
  crawl_data.products                    # Main table (updated in-place)
    └── Columns: is_clean, cleaned_at, quality_score, has_anomaly, anomaly_flags
  
  crawl_data.rejected_products           # Failed records
    └── product_id, rejection_reason, rejection_details, rejected_at
  
  crawl_data.cleaning_audit              # Audit trail
    └── product_id, transformation_type, old_value, new_value, cleaned_at
  
  crawl_data.cleaning_stats              # Time-series metrics
    └── run_date, total_processed, clean_count, rejected_count, avg_quality
```

---

## Implementation Checklist

### Setup (Week 1)
- [ ] Create DAG skeleton với TaskGroups
- [ ] Implement base validator class
- [ ] Setup XCom data passing
- [ ] Configure Airflow Variables

### Core Features (Week 2-3)
- [ ] Implement validation & filtering tasks
- [ ] Implement normalization tasks
- [ ] Implement enrichment tasks
- [ ] Add anomaly detection

### Advanced Features (Week 4)
- [ ] Implement segmentation
- [ ] Add quality assurance checks
- [ ] Build HTML report generator
- [ ] Setup notifications

### Testing & Deployment (Week 5)
- [ ] Unit tests for all modules
- [ ] Integration tests
- [ ] Performance benchmarking
- [ ] Deploy to production

---

## Tài liệu tham khảo
- Transformer hiện tại: `src/pipelines/transform/transformer.py`
- Data schema: `docs/products_final_fields_vi.md`
- Crawl DAG: `airflow/dags/tiki_crawl_products_dag.py`
- Quality standards: ISO 8000 (Data Quality)
