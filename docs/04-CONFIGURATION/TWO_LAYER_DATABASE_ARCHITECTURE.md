# 🏗️ Two-Layer Database Architecture — Tiki Data Pipeline

## Overview

The Tiki data pipeline uses a **two-layer database architecture** for separation of concerns:

```
┌──────────────────────────────────────────────────────────────┐
│                     LAYER 1: ETL (OLTP)                      │
│                    Database: crawl_data                       │
│                                                               │
│  • Normalized schema (3NF)                                    │
│  • Products, Categories, Sellers, Reviews                    │
│  • Raw data from Selenium crawlers                            │
│  • ~40+ fields per product (specs, images JSONB)             │
│  • Designed for INSERT/UPDATE operations                     │
│  • Source of truth for crawled data                          │
│                                                               │
└──────────────────────────────────────────────────────────────┘
                              │
                              │ ETL Process
                              │ (StarSchemaBuilderV2)
                              │
┌──────────────────────────────────────────────────────────────┐
│                   LAYER 2: WAREHOUSE (OLAP)                  │
│                 Database: tiki_warehouse                      │
│                                                               │
│  • Star Schema (Kimball methodology)                          │
│  • 1 Fact Table + 6 Dimension Tables                         │
│  • Optimized for complex analytical queries                  │
│  • Denormalized for BI/Analytics performance                 │
│  • Surrogate keys (SK) for dimension tables                  │
│  • Foreign keys to maintain referential integrity            │
│  • Pre-built SQL views for common OLAP queries               │
│                                                               │
│  Fact Table:                                                 │
│    ├─ fact_product_sales (16 columns)                       │
│      ├─ Price metrics (price, original_price, discount%)    │
│      ├─ Sales metrics (quantity, revenue, profit)           │
│      └─ Rating metrics (avg_rating, rating_count, reviews)  │
│                                                               │
│  Dimension Tables:                                           │
│    ├─ dim_product (6 cols: SK, product_id, name, brand)    │
│    ├─ dim_category (8 cols: SK, id, path, 5 levels)        │
│    ├─ dim_seller (3 cols: SK, seller_id, name)             │
│    ├─ dim_brand (2 cols: SK, brand_name)                   │
│    ├─ dim_date (5 cols: SK, date, year, month, day)        │
│    └─ dim_price_segment (4 cols: SK, name, min, max)       │
│                                                               │
└──────────────────────────────────────────────────────────────┘
```

---

## Layer 1: ETL (OLTP) — `crawl_data` Database

### Purpose
- Primary data store for crawled, transformed, and loaded products
- Normalized structure for data integrity
- Source of truth before dimensional transformation

### Main Tables

| Table | Records | Purpose |
|-------|---------|---------|
| **products** | 10K-100K+ | Main product data with 40+ fields |
| **categories** | ~1,000 | Category reference (5-level hierarchy) |
| **sellers** | ~500 | Seller reference data |
| **product_reviews** | 100K+ | Customer reviews (optional) |

### Key Fields in `products` Table

**Identifiers:**
- `product_id` (UNIQUE): External product identifier
- `url`: Product page URL

**Classification:**
- `category_id`, `category_path` (JSONB): Category hierarchy
- `category_url`: Category page URL

**Pricing:**
- `price`: Current selling price
- `original_price`: Original/list price
- `discount_percent`: Calculated discount
- `price_category`: Price segment label

**Sales & Popularity:**
- `sales_count`: Estimated number of sales
- `sales_velocity`: Sales per unit time
- `popularity_score`: Calculated metric
- `value_score`: Price-to-quality metric
- `estimated_revenue`: Calculated from price × sales

**Ratings & Reviews:**
- `rating_average`: Average customer rating (1-5)
- `review_count`: Number of customer reviews
- `sales_count`: Number of confirmed purchases

**Details:**
- `brand`: Brand name
- `description`: Product description text
- `specifications` (JSONB): Product specs (attributes, values)
- `images` (JSONB): Product image URLs

**Seller Info:**
- `seller_name`, `seller_id`: Seller reference
- `seller_is_official`: Boolean flag

**Stock & Shipping:**
- `stock_available`: In-stock flag
- `stock_quantity`: Available quantity
- `stock_status`: Status enum
- `shipping` (JSONB): Shipping details

**Audit:**
- `crawled_at`: When data was crawled
- `updated_at`: When record was last updated

### Upsert Strategy
- Uses `ON CONFLICT (product_id) DO UPDATE`
- Idempotent: safe to re-run without duplicates
- Prevents duplicate product entries

---

## Layer 2: WAREHOUSE (OLAP) — `tiki_warehouse` Database

### Purpose
- Analytical data warehouse for BI/reporting
- Dimensional schema optimized for complex queries
- Denormalized structure for query performance
- Pre-aggregated data and metrics

### Architecture: Star Schema

**Fact Table:** `fact_product_sales`
- Central table with transactional data
- Foreign keys to all 6 dimensions
- Metrics: price, revenue, profit, ratings

**Dimension Tables:**
- `dim_product`: Product master data (6 cols)
- `dim_category`: Category hierarchy (8 cols)
- `dim_seller`: Seller reference (3 cols)
- `dim_brand`: Brand reference (2 cols)
- `dim_date`: Date decomposition (5 cols)
- `dim_price_segment`: Price ranges (4 cols)

### Schema Benefits

| Benefit | Why |
|---------|-----|
| **Fast analytical queries** | Denormalized; fewer joins |
| **Aggregation support** | Facts pre-calculated; dims support GROUP BY |
| **Query optimization** | Indexes on fact FKs and dimensions |
| **Clear semantics** | Business dimensions explicitly modeled |
| **Reusability** | Common views (vw_*) for standard reports |

### Surrogate Keys

All dimension tables use surrogate keys (`*_sk`):
- Decouples business key from storage
- Stable references even if source data changes
- Smaller foreign keys in fact table
- Example: `product_sk` in `dim_product` → referenced in `fact_product_sales`

### Price Segmentation

`dim_price_segment` provides 6 fixed segments:

| Segment | Min Price | Max Price | Use Case |
|---------|-----------|-----------|----------|
| Chưa cập nhật | NULL | NULL | Missing data |
| Rẻ | 0 | 100,000 | Budget products |
| Bình dân | 100,000 | 500,000 | Mid-range |
| Trung bình | 500,000 | 1,000,000 | Higher-end |
| Cao | 1,000,000 | 5,000,000 | Premium |
| Cao cấp | 5,000,000 | NULL | Luxury |

### Date Dimension

`dim_date` enables time-based analysis without calculation:
- `date_sk`: Surrogate key
- `date_value`: Date (UNIQUE)
- `year`, `month`, `day`: Decomposed for easy filtering

### Fact Table Metrics

| Column | Type | Purpose |
|--------|------|---------|
| `price` | NUMERIC(12,2) | Current selling price |
| `original_price` | NUMERIC(12,2) | Original list price |
| `discount_percent` | NUMERIC(5,2) | Discount % |
| `quantity_sold` | INT | Units sold |
| `estimated_revenue` | NUMERIC(15,2) | Total revenue (price × qty) |
| `estimated_profit` | NUMERIC(15,2) | Calculated profit margin |
| `average_rating` | NUMERIC(3,1) | Avg customer rating |
| `rating_count` | INT | Total ratings received |
| `review_count` | INT | Total written reviews |

---

## ETL Process: crawl_data → tiki_warehouse

### Tool: `StarSchemaBuilderV2` (in `src/pipelines/warehouse/star_schema_builder.py`)

```python
class StarSchemaBuilderV2:
    def __init__(self, db_host, db_port, source_db="crawl_data", target_db="tiki_warehouse"):
        # Connect to both databases
        # source_db: ETL layer (normalized)
        # target_db: Warehouse layer (dimensional)
    
    def create_schema(self):
        # Create all 7 tables with proper relationships
        # 1. Create all dimension tables first
        # 2. Load reference data (price_segments, dates, brands)
        # 3. Create fact table with FK constraints
        # 4. Add indexes for query performance
    
    def load_data(self):
        # Extract from crawl_data.products
        # Transform to dimensional model
        # Load into warehouse
        # 1. Lookup/create dimension records
        # 2. Insert fact records
```

### Extraction
- Query `crawl_data.products` table
- Extract ~40 fields per product
- Handle NULL/missing values

### Transformation
- Denormalize product data into fact + dimensions
- Map dimensions:
  - Product → `dim_product`
  - Category path → `dim_category` (5 levels)
  - Seller → `dim_seller`
  - Brand → `dim_brand`
  - Create date → `dim_date`
  - Price range → `dim_price_segment`
- Calculate/aggregate metrics:
  - Revenue = price × quantity
  - Profit = revenue × margin
  - Segment = bucket price into 6 ranges

### Loading
- Insert/update dimension records (UPSERT)
- Insert fact records (append-only)
- Maintain referential integrity via FKs
- Build indexes for query optimization

---

## SQL Views for Common Analysis

Pre-built views for standard BI queries:

### `vw_top_products_revenue`
- Top products by revenue
- Includes brand, seller, rating

### `vw_category_performance`
- Performance by category (1st and 2nd level)
- Metrics: product count, revenue, rating, quantity

### `vw_daily_sales`
- Daily sales metrics
- Grouped by date with year/month/day

### `vw_price_segment_analysis`
- Analysis by price segment
- Metrics: product count, avg price, revenue, rating, discount

---

## Comparison: Layer 1 vs Layer 2

| Aspect | Layer 1 (crawl_data) | Layer 2 (tiki_warehouse) |
|--------|---------------------|-------------------------|
| **Type** | OLTP (normalized) | OLAP (denormalized) |
| **Schema** | 3NF (normalized) | Star Schema (dimensional) |
| **Primary Key** | business keys | Surrogate keys (SK) |
| **Table Count** | 3-5 | 7 (1 fact + 6 dims) |
| **Query Style** | JOIN-heavy | Simple table scans + aggregation |
| **Update Pattern** | Frequent INSERT/UPDATE | Dimension UPSERT, Fact INSERT-only |
| **Typical Query** | "Find product by ID" | "Total revenue by category this month" |
| **Performance** | Good for transactional | Excellent for analytical |
| **Data Size** | 1 table with 40+ cols | 1 large fact + 6 small dims |
| **Documentation** | See `CRAWL_DATA_SCHEMA.md` | See `TIKI_WAREHOUSE_DATABASE_SCHEMA_VI.md` |

---

## Connection Strings

### Layer 1: `crawl_data`
```
postgresql://user:password@localhost:5432/crawl_data
```

### Layer 2: `tiki_warehouse`
```
postgresql://user:password@localhost:5432/tiki_warehouse
```

---

## Files to Review

- **ETL Code:** `src/pipelines/warehouse/star_schema_builder.py` (629 lines)
  - StarSchemaBuilderV2 class
  - `create_schema()`: Create all 7 tables
  - `load_data()`: Transform and load

- **Layer 1 Schema Docs:** (if exists)
  - `crawl_data` normalized schema reference

- **Layer 2 Schema Docs:** 
  - `docs/04-CONFIGURATION/TIKI_WAREHOUSE_DATABASE_SCHEMA_VI.md` (1430+ lines)
  - Complete Star Schema documentation
  - SQL DDL statements for all 7 tables
  - SQL views for common analysis

- **Airflow Integration:**
  - `airflow/dags/tiki_*.py` DAGs orchestrate warehouse updates
  - DAGs trigger StarSchemaBuilderV2 on schedule

---

## Getting Started

### To verify the actual warehouse schema:
```bash
# Connect to PostgreSQL
psql -U $POSTGRES_USER -h localhost -d tiki_warehouse

# List tables
\dt public.

# Describe fact table
\d fact_product_sales

# Describe dimensions
\d dim_product
\d dim_category
# ... etc
```

### To inspect warehouse data:
```sql
-- Check row counts
SELECT 'fact_product_sales' as table_name, COUNT(*) as row_count 
FROM fact_product_sales
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_category', COUNT(*) FROM dim_category;

-- Top 5 products by revenue
SELECT * FROM vw_top_products_revenue LIMIT 5;

-- Daily sales trend
SELECT * FROM vw_daily_sales ORDER BY date_value DESC LIMIT 30;
```

### To rebuild warehouse from source:
```python
from src.pipelines.warehouse.star_schema_builder import StarSchemaBuilderV2

builder = StarSchemaBuilderV2(db_host='localhost', db_port=5432)
builder.connect()
builder.create_schema()
builder.load_data()
```

---

## Summary

- **Two layers ensure separation of concerns:**
  - Layer 1 (ETL): Normalized, transactional, crawled data
  - Layer 2 (Warehouse): Denormalized, analytical, dimensional model

- **Star Schema provides analytics power:**
  - Fast queries via denormalization
  - Clear business dimensions
  - Pre-aggregated metrics and views

- **Surrogate keys + FK constraints maintain data quality**

- **Both layers documented in detail** for understanding and maintenance
