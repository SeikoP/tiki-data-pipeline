# Airflow Nodes Cho Analytics Normalize

Tài liệu này giải thích DAG `tiki_analytics_normalize` sau khi chuyển analytics
schema sang hướng **current snapshot only**.

## 1. Mục Tiêu DAG

DAG này không crawl dữ liệu mới. Nó chỉ đọc các bảng core đã load xong và tạo
bảng phân tích sạch:

```text
public.products
public.categories
public.sellers
-> analytics.product_analysis_ready
```

DAG không tạo:

- Daily snapshot fact.
- Price-change tracking.
- Crawl quality report.
- Summary table theo category/seller.
- Seller `unknown`.

## 2. Trigger

`tiki_analytics_normalize` được schedule theo asset `TIKI_CORE_TABLES_ASSET`.
Asset này được publish sau khi DAG crawl/load chính hoàn tất thành công. Vì vậy
normalize chỉ chạy khi core product tables đã có dữ liệu mới.

## 3. Node `resolve_analytics_context`

Mục đích:

- Đọc schema analytics từ config `TIKI_ANALYTICS_SCHEMA`.
- Đọc `crawl_run_id` trong `dag_run.conf` nếu có.

Lưu ý:

- `crawl_run_id` chỉ dùng để trace nếu cần.
- Schema analytics hiện tại không lưu crawl quality hoặc trạng thái run.

Input:

- Airflow context.
- Airflow Variable/env setting `TIKI_ANALYTICS_SCHEMA`.

Output:

```json
{
  "schema": "analytics",
  "crawl_run_id": 123
}
```

## 4. Node `ensure_analytics_schema`

Mục đích:

- Tạo schema `analytics` nếu chưa tồn tại.
- Drop các bảng/view analytics legacy không còn dùng.
- Tạo lại bảng `analytics.product_analysis_ready` với cấu trúc sạch.
- Tạo index cho các cột hay filter/group: `home_segment`, `seller_id`,
  `category_id`.

Vì analytics là derived layer, việc recreate bảng là chấp nhận được: dữ liệu
được sinh lại từ core tables ở node tiếp theo.

Các object legacy bị loại:

- `fact_product_daily`
- `fact_category_daily`
- `fact_seller_daily`
- `fact_price_change`
- `fact_crawl_quality`
- `dim_product`
- `dim_category`
- `dim_seller`
- `mart_*`
- `category_summary`
- `seller_summary`

## 5. Node `refresh_product_analysis_ready`

Mục đích:

- Join `products`, `categories`, `sellers`.
- Chuẩn hóa category thành `market_scope` và `home_segment`.
- Loại sản phẩm không đủ điều kiện phân tích.
- Tính các bucket tiếng Việt trực tiếp trong cột gốc.
- Ghi 1 row cho 1 sản phẩm hiện tại vào `product_analysis_ready`.

Điều kiện giữ sản phẩm:

- Có `product_id`.
- Có `product_name`.
- Có `product_url`.
- Có `price > 0`.
- Có category hợp lệ.
- Category thuộc segment phân tích chính.
- Có `seller_id`.
- Có `seller_name`.

Các bucket được tạo:

- `price_bucket`
- `discount_bucket`
- `rating_bucket`
- `review_bucket`
- `sold_bucket`
- `market_position`

Tất cả bucket dùng giá trị tiếng Việt trực tiếp, ví dụ `Giá TB`, `KM cao`,
`Rating 0`, `0 review`, `Sold TB`, `Giá tốt`.

## 6. Dependency Graph

```text
resolve_analytics_context
-> ensure_analytics_schema
-> refresh_product_analysis_ready
```

Không còn node daily/summary/crawl-quality vì các node đó không phục vụ trực
tiếp mục tiêu phân tích sản phẩm hiện tại.
