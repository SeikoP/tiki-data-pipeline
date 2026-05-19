# Tiki Data Pipeline

`tiki-data-pipeline` là pipeline dữ liệu phục vụ phân tích thị trường sản phẩm
tiện ích gia đình trên Tiki. Dự án thu thập dữ liệu sản phẩm, chuẩn hóa qua các
lớp dữ liệu, lưu vào PostgreSQL và tạo bảng analytics sạch để dùng trực tiếp
trong Power BI hoặc SQL analysis.

Trọng tâm của dự án không chỉ là crawl dữ liệu, mà là biến dữ liệu thương mại
điện tử bán cấu trúc thành dataset phân tích được: có định danh ổn định, category
được gom nhóm, seller được chuẩn hóa, metric giá/rating/review/sold được làm
sạch và các bucket phân tích được Việt hóa ngắn gọn.

## Bài Toán Phân Tích

Dự án hướng tới các câu hỏi:

- Nhóm sản phẩm tiện ích gia đình nào có nhiều sản phẩm nhất trên Tiki?
- Phân khúc giá nào phổ biến trong từng nhóm hàng?
- Mức giảm giá có đi kèm rating/review tốt hơn không?
- Seller official và marketplace khác nhau thế nào về giá, rating và lượng bán?
- Sản phẩm nào có tín hiệu cơ hội tốt theo giá, review, rating và khuyến mãi?

Phạm vi phân tích hiện tập trung vào các nhóm hàng gia đình như `Bếp`, `Vệ sinh`,
`Giặt ủi`, `Lưu trữ`, `Nhà tắm`, `Tiện nghi` và `Trang trí`.

## Kiến Trúc Dữ Liệu

```text
Tiki API
-> raw files / raw_products
-> core normalized tables
-> analytics dimensions + snapshot facts
-> analytics.product_analysis_ready view
-> Power BI / SQL analysis / báo cáo
```

Các lớp chính:

- **Raw layer**: giữ payload crawl gốc để audit và debug.
- **Core normalized layer**: lưu sản phẩm, category, seller, price/metric snapshot
  trong PostgreSQL.
- **Analytics layer**: tạo star schema gồm dimensions, snapshot facts và view
  `analytics.product_analysis_ready` để đọc nhanh.

Analytics schema hiện tại theo hướng **snapshot-based star schema**. Dự án không
giả định crawl hằng ngày, nên không tạo bảng `daily`; lịch sử giá được lưu theo
từng snapshot crawl/normalize qua `fact_price_snapshot`.

## Airflow DAGs

| DAG | Vai trò |
| --- | --- |
| `tiki_crawl_fast` | Crawl category/product list/detail, transform và load vào core tables |
| `tiki_analytics_normalize` | Tạo dimensions, snapshot facts và view phân tích sau khi core tables cập nhật |

`tiki_crawl_fast` dùng file output cho payload lớn và chỉ đẩy metadata qua XCom
để tránh làm phình Airflow metadata database.

`tiki_analytics_normalize` hiện chỉ gồm ba node chính:

```text
resolve_analytics_context
-> ensure_analytics_schema
-> refresh_dimensions
-> refresh_snapshot
-> refresh_product_snapshot_fact
-> refresh_price_snapshot_fact
```

## Analytics Output

Các bảng chính cho phân tích:

```text
analytics.dim_snapshot
analytics.dim_category
analytics.dim_seller
analytics.dim_product
analytics.fact_product_snapshot
analytics.fact_price_snapshot
analytics.product_analysis_ready -- view join sẵn cho Power BI/SQL nhanh
```

Đặc điểm:

- Fact grain = 1 sản phẩm tại 1 snapshot crawl/normalize.
- Không có seller giả `unknown`; sản phẩm thiếu seller hợp lệ bị loại.
- Không có cột `*_label`; các cột phân loại chứa tiếng Việt trực tiếp.
- Không có bảng summary phụ; Power BI hoặc SQL có thể `GROUP BY` từ fact/view.

Nhóm cột tiêu biểu:

- Product: `product_id`, `product_name`, `short_name`, `brand`, `product_url`.
- Category: `category_id`, `category_name`, `category_path`, `home_segment`,
  `market_scope`.
- Seller: `seller_id`, `seller_name`, `seller_type`.
- Metrics: `price`, `original_price`, `discount_percent`, `rating_average`,
  `review_count`, `sales_count`.
- Buckets: `price_bucket`, `discount_bucket`, `rating_bucket`, `review_bucket`,
  `sold_bucket`, `market_position`.

Ví dụ bucket dùng trực tiếp trong dashboard: `Giá TB`, `KM cao`, `Rating 0`,
`0 review`, `Sold TB`, `Giá tốt`.

## Thành Phần Code Chính

| Đường dẫn | Vai trò |
| --- | --- |
| `airflow/dags/tiki_crawl_products_v2/` | DAG crawl/load sản phẩm Tiki |
| `airflow/dags/tiki_analytics_normalize.py` | DAG normalize analytics current snapshot |
| `src/common/` | Settings, schema, tracking, cache và tiện ích chung |
| `src/pipelines/crawl/` | Crawl category, product list và product detail |
| `src/pipelines/transform/` | Chuẩn hóa dữ liệu sản phẩm |
| `src/pipelines/load/` | Load dữ liệu vào PostgreSQL |
| `src/pipelines/analytics/` | Tạo dimensions, snapshot facts và view phân tích |
| `tests/` | Test hợp đồng dữ liệu, DAG import, transform/load và profiling |
| `docs/` | Tài liệu kỹ thuật và cấu hình |
| `graduation_docs/` | Tài liệu đồ án, data dictionary và query dashboard |

## Công Nghệ

- Python 3.11
- Apache Airflow
- PostgreSQL 16
- Redis 7.2
- Docker Compose
- requests/aiohttp, BeautifulSoup/lxml
- psycopg2, pandas/numpy
- pytest, Ruff, mypy

## Tài Liệu Liên Quan

- [docs/INDEX.md](docs/INDEX.md): tài liệu kỹ thuật tổng quan.
- [docs/01-PARAMETERS/README.md](docs/01-PARAMETERS/README.md): tham số cấu hình.
- [src/pipelines/crawl/README.md](src/pipelines/crawl/README.md): crawl module.
- [tests/README.md](tests/README.md): hướng dẫn test.
- `graduation_docs/05_analytics_schema_data_dictionary.md`: schema analytics.
- `graduation_docs/07_sql_queries_and_vietnamese_aliases.md`: query dashboard.
- `graduation_docs/09_airflow_analytics_nodes.md`: cách hoạt động từng node
  Airflow normalize.

## Ghi Chú Vận Hành

Các lệnh chạy local, CI, cấu hình và cleanup nằm trong `docs/` và `scripts/`.
Runtime artifacts như `data/`, `backups/`, Airflow logs, cache và Docker volumes
không phải là mã nguồn. Không dùng `docker compose down -v` nếu muốn giữ dữ liệu
PostgreSQL và Redis cache.
