# Airflow Nodes Cho Analytics Normalize

DAG `tiki_analytics_normalize` tạo mô hình analytics theo snapshot. DAG này
không crawl dữ liệu mới; nó đọc core tables và build dimensions/facts.

## 1. Dependency Graph

```text
resolve_analytics_context
-> ensure_analytics_schema
-> refresh_dimensions
-> refresh_snapshot
-> refresh_product_snapshot_fact
-> refresh_price_snapshot_fact
```

Hai fact cuối có thể chạy song song sau khi snapshot đã tồn tại.

## 2. Node `resolve_analytics_context`

Mục đích:

- Đọc schema từ `TIKI_ANALYTICS_SCHEMA`.
- Đọc `crawl_run_id` nếu DAG được trigger kèm conf.

Output mẫu:

```json
{
  "schema": "analytics",
  "crawl_run_id": 123
}
```

## 3. Node `ensure_analytics_schema`

Mục đích:

- Tạo schema analytics nếu chưa tồn tại.
- Tạo các bảng:
  - `dim_snapshot`
  - `dim_category`
  - `dim_seller`
  - `dim_product`
  - `fact_product_snapshot`
  - `fact_price_snapshot`
- Tạo view `product_analysis_ready`.
- Drop các bảng/view legacy không còn dùng như `fact_*_daily`,
  `fact_crawl_quality`, `fact_price_change`, `mart_*`.

Node này không drop các bảng snapshot/fact mới, để còn giữ lịch sử theo snapshot.

## 4. Node `refresh_dimensions`

Mục đích:

- Refresh `dim_category` từ core `categories`.
- Refresh `dim_seller` từ core `sellers` và fallback seller trong `products`.
- Refresh `dim_product` từ core `products`.

Quy tắc:

- Không tạo seller `unknown`.
- Product thiếu seller/category hợp lệ không được load vào `dim_product`.
- Category ngoài phạm vi được giữ trong `dim_category`, nhưng `dim_product` chỉ
  nhận category có `is_analysis_target = true`.

## 5. Node `refresh_snapshot`

Mục đích:

- Tạo hoặc reuse dòng `dim_snapshot` đại diện cho core data hiện tại.
- Grain là lần crawl/normalize snapshot, không phải ngày.

Snapshot được xác định từ thời điểm mới nhất trong `products.crawled_at` hoặc
`products.updated_at`.

## 6. Node `refresh_product_snapshot_fact`

Mục đích:

- Ghi `fact_product_snapshot` cho snapshot hiện tại.
- Chứa rating, review, sold, bucket tương ứng, `value_score` và
  `market_position`.

Grain:

```text
1 row = 1 product + 1 snapshot
```

## 7. Node `refresh_price_snapshot_fact`

Mục đích:

- Ghi `fact_price_snapshot` cho snapshot hiện tại.
- Chứa price, original price, discount, `price_bucket`, `discount_bucket`.

Grain:

```text
1 row = 1 product + 1 snapshot
```

Lịch sử giá được track bằng nhiều snapshot. Khi cần tính biến động giá, dùng SQL
`LAG()` trên `fact_price_snapshot`, không cần bảng `fact_price_change` riêng.
