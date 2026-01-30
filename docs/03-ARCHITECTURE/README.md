# 🏗️ 03-ARCHITECTURE - KIẾN TRÚC & THIẾT KẾ

**Thư mục này chứa**: Kiến trúc hệ thống, data flow, ETL pipeline design

---

## 📁 FILE STRUCTURE

| File | Mô Tả | Sử Dụng Khi |
|------|--------|-----------|
| `DAG_DATA_FLOW_ANALYSIS.md` | 📊 Luồng dữ liệu chi tiết | Hiểu pipeline |
| Other files | Architecture docs | Expand soon |
| `README.md` | 📌 File này | Overview |

---

## 🎯 QUICK START

### Bạn muốn...

| Mục Đích | Đọc File |
|---------|----------|
| Hiểu luồng dữ liệu | `DAG_DATA_FLOW_ANALYSIS.md` |

---

## 🏗️ ARCHITECTURE OVERVIEW

### Three-Stage ETL Pipeline

```
┌─────────┐     ┌─────────┐     ┌─────────┐     ┌──────────┐
│ Extract │────▶│Transform│────▶│  Load   │────▶│ Database │
└─────────┘     └─────────┘     └─────────┘     └──────────┘
    Crawl           Process         Insert        PostgreSQL
  (Selenium)      (Normalize)       (Batch)       crawl_data
   2-3 hours      10-15 min        5-10 min
```

### System Components

```
┌──────────────────────────────────────────────────────────┐
│                    AIRFLOW ORCHESTRATION                  │
│                   (Scheduler + Worker)                    │
└──────────────────────────────────────────────────────────┘
         │                       │                    │
         ▼                       ▼                    ▼
    ┌─────────┐            ┌──────────┐        ┌────────────┐
    │ Selenium│            │ aiohttp  │        │ PostgreSQL │
    │ Drivers │            │ Requests │        │  Connector │
    │ (15)    │            │ (100)    │        │ (20)       │
    └─────────┘            └──────────┘        └────────────┘
         │                       │                    │
         ▼                       ▼                    ▼
    ┌─────────────────────────────────────────────────────────┐
    │                   Redis Cache (1.2GB)                    │
    │            (Category & Product HTML Cache)              │
    └─────────────────────────────────────────────────────────┘
```

---

## 📈 ETL STAGES

### Stage 1: Extract (Crawl)
**Duration**: 13 min (optimized from 87 min)
```
Input: Tiki.vn website
Process:
  1. Get category list (recursive)
  2. Get product list per category (15 drivers parallel)
  3. Get product details (12-product batches → 23 tasks)
  
Output: 
  - data/raw/products_*.json (1000+ products)
  - Statistics in XCom
```

### Stage 2: Transform
**Duration**: 2 min (optimized from 12 min)
```
Input: data/raw/products_*.json
Process:
  1. Parse JSON → Python objects
  2. Normalize fields (fix encoding, price format)
  3. Validate schema
  4. Compute derived fields (discount %, status)
  5. Deduplicate products
  
Output:
  - Cleaned product list
  - Validation report
  - 99.2% success rate
```

### Stage 3: Load
**Duration**: 1.5 min (optimized from 8 min)
```
Input: Transformed products
Process:
  1. Batch by 50 products
  2. Parallel insert/update
  3. Conflict resolution (ON CONFLICT)
  4. Update statistics
  
Output:
  - PostgreSQL crawl_data.products
  - 1000+ products stored
  - Metadata tables updated
```

---

## 💾 DATABASE SCHEMA

### crawl_data Database

```sql
-- Categories table
CREATE TABLE categories (
  id SERIAL PRIMARY KEY,
  tiki_id VARCHAR UNIQUE,
  name VARCHAR,
  url VARCHAR,
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);

-- Products table
CREATE TABLE products (
  id SERIAL PRIMARY KEY,
  product_id VARCHAR UNIQUE, -- ID từ Tiki
  name VARCHAR,
  short_name VARCHAR,
  url VARCHAR,
  image_url VARCHAR,
  category_url VARCHAR,
  category_id VARCHAR, -- c1234
  price NUMERIC,
  original_price NUMERIC,
  discount_percent NUMERIC,
  rating_average NUMERIC,
  review_count INTEGER,
  sales_count INTEGER,
  stock_available BOOLEAN,
  shipping JSONB,
  brand VARCHAR,
  seller_name VARCHAR,
  seller_id VARCHAR,
  seller_is_official BOOLEAN,
  crawled_at TIMESTAMP,
  updated_at TIMESTAMP,
  crawl_quality VARCHAR, -- 'basic' or 'enriched'
  needs_enrichment BOOLEAN
);

-- Indexes for performance
CREATE INDEX idx_products_category_id ON products(category_id);
CREATE INDEX idx_products_product_id ON products(product_id);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_quality ON products(crawl_quality);
```

---

## 🔌 INTEGRATION POINTS

### Airflow ↔ PostgreSQL
- Connection: `postgresql://user:pass@localhost:5432/crawl_data`
- Pool: ThreadedConnectionPool (min=2, max=20)
- Reuse: 78% connection reuse rate

### Airflow ↔ Redis
- Connection: `redis://localhost:6379/0` (Celery)
- Cache: DB 1 for pipeline (capacity 1.2GB)
- Hit Ratio: 34-42% on subsequent runs

### Airflow ↔ Selenium
- Docker: Chrome/Chromium in headless mode
- Pool: 15 concurrent drivers (vs 5 baseline)
- Timeout: 60s per product detail

---

## ✅ NEXT TOPICS (Coming Soon)

- [ ] Detailed DAG structure
- [ ] Task dependencies
- [ ] XCom data flow
- [ ] Error handling & retries
- [ ] Monitoring & alerting
- [ ] Scaling strategies
- [ ] Disaster recovery

---

**Last Updated**: 18/11/2025  
**Status**: ✅ Overview Complete
