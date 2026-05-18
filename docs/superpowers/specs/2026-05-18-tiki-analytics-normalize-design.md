# Tiki Analytics Normalize DAG Design

## Goal

Build a downstream Airflow DAG that turns the basic loaded crawl tables into
analysis-ready data for a Data Analytics graduation project.

## Scope

The new DAG does not crawl Tiki again and does not replace `transform_and_load`.
It reads from the existing PostgreSQL tables:

- `products`
- `categories`
- `sellers`
- `product_prices`
- `product_metrics`
- `crawl_history`
- `crawl_runs`
- `crawl_tasks`
- `raw_products`

It writes normalized analytics tables into a dedicated PostgreSQL schema,
defaulting to `analytics`.

## Architecture

Add a small analytics module under `src/pipelines/analytics/` and a new DAG file
under `airflow/dags/`. The DAG uses lightweight wrappers at parse time and imports
the analytics module only inside task callables.

The analytics module owns:

- idempotent analytics schema creation
- dimension refresh
- daily fact refresh
- aggregate fact refresh
- run quality refresh

The DAG owns orchestration only:

1. resolve optional `crawl_run_id` from `dag_run.conf`
2. ensure analytics schema
3. refresh dimensions
4. refresh product daily facts
5. refresh aggregate facts
6. refresh price change facts
7. refresh crawl quality facts

## Output Tables

The DAG creates and refreshes:

- `analytics.dim_category`
- `analytics.dim_seller`
- `analytics.dim_product`
- `analytics.fact_product_daily`
- `analytics.fact_category_daily`
- `analytics.fact_seller_daily`
- `analytics.fact_price_change`
- `analytics.fact_crawl_quality`
- `analytics.product_current`

`product_current` is a table, not a view, so BI tools can query it without
complex joins.

## Data Semantics

`dim_*` tables keep stable descriptive fields. Missing enrichment fields remain
`NULL` instead of being converted to fake zero values.

`fact_product_daily` stores one latest product snapshot per product per day. If
multiple crawl runs happen on the same day, the latest snapshot overwrites the
previous row for that product/date.

`fact_category_daily` and `fact_seller_daily` are rebuilt for affected snapshot
dates from `fact_product_daily`.

`fact_price_change` mirrors price change events from `crawl_history`.

`fact_crawl_quality` summarizes each crawl run for reporting crawl volume,
task success/failure, raw payload count, and detail quality.

## Configuration

Add `TIKI_ANALYTICS_SCHEMA` to the central settings registry. It defaults to
`analytics`, is available as an Airflow Variable, and is documented in
`.env.example`.

The schema name is validated before SQL execution so it cannot contain arbitrary
SQL.

## Error Handling

Each task runs in a database transaction through the existing PostgreSQL pool.
Schema creation is idempotent. Refresh tasks are repeatable and use UPSERT or
delete-and-reinsert for affected aggregate dates.

If no product snapshots exist, the DAG succeeds with zero inserted rows and logs
the empty result instead of failing.

## Testing

Add focused unit tests for:

- analytics schema name validation
- generated analytics DDL coverage
- DAG import without Airflow installed
- config registry availability

Run the existing config sync check and focused pytest tests.
