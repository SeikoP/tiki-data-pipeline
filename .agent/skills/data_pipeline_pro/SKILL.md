---
name: Scalable Data Pipeline
description: Standardized workflows for transforming raw crawl data, validating schemas, and high-performance loading to PostgreSQL.
---

# Scalable Data Pipeline

This skill handles the "TL" (Transform & Load) parts of the ETL pipeline, ensuring data quality and database performance.

## Key Workflows

### 1. Data Transformation (`DataTransformer`)
- Normalizes product prices, sales counts, and brand names.
- Shortens long product names using smart heuristics.
- Enriching `category_path` by matching product `category_id` with the master category tree.

### 2. High-Performance Loading (`OptimizedDataLoader`)
- Uses PostgreSQL batch inserts (`COPY` or multi-row `INSERT`) to maximize throughput.
- Implements `UPSERT` (Insert on conflict update) logic to maintain fresh products without creating duplicates.
- Handles database connection pooling through `psycopg2`.

### 3. Schema Management
- Maintains `products` and `categories` tables.
- Updates schemas automatically (e.g., adding `review_count`, `crawl_quality`, `needs_enrichment`).
- Management of database backups and history archiving.

## Quality Control
- **Validation**: Filters out products missing critical fields (price, seller, brand).
- **Monitoring**: Aggregates crawl stats and sends summary reports via Discord/AI logs.
- **Consistency**: Reconciles category paths and rebuilds missing parent categories.

## Performance Tuning
- `TIKI_SAVE_BATCH_SIZE`: Optimized at 2000-5000 records.
- Indexing: Ensures `product_id` and `url` are indexed for fast lookups.
