# Tiki Data Pipeline

`tiki-data-pipeline` là pipeline dữ liệu phục vụ phân tích thị trường sản phẩm
trên Tiki, trọng tâm là nhóm hàng tiện ích gia đình. Dự án thu thập dữ liệu sản
phẩm, chuẩn hóa qua các lớp dữ liệu, lưu vào PostgreSQL và tạo bộ dữ liệu phân
tích có thể dùng trực tiếp trong SQL, Power BI hoặc báo cáo đồ án.

Trọng tâm của dự án không chỉ là crawl dữ liệu. Pipeline biến dữ liệu thương mại
điện tử bán cấu trúc thành dataset có định danh ổn định, category được gom nhóm,
seller được chuẩn hóa, metric giá/rating/review/sold được làm sạch và các bucket
phân tích được Việt hóa ngắn gọn.

## Bài Toán Phân Tích

Dự án hướng tới các câu hỏi:

- Nhóm sản phẩm tiện ích gia đình nào có nhiều sản phẩm nhất?
- Phân khúc giá nào phổ biến trong từng nhóm hàng?
- Khuyến mãi có đi kèm rating, review hoặc lượng bán tốt hơn không?
- Seller official và marketplace khác nhau thế nào?
- Sản phẩm nào có tín hiệu cơ hội theo giá, rating, review và khuyến mãi?

Phạm vi hiện tập trung vào các nhóm hàng gia đình như `Bếp`, `Vệ sinh`,
`Giặt ủi`, `Lưu trữ`, `Nhà tắm`, `Tiện nghi` và `Trang trí`.

## Luồng Dữ Liệu

```text
Tiki API
-> raw crawl files / raw_products
-> core normalized tables
-> analytics dimensions + snapshot facts
-> product_analysis_ready
-> Power BI / SQL analysis / báo cáo
```

Các lớp chính:

- **Raw layer**: giữ payload crawl gốc để audit và debug.
- **Core layer**: lưu sản phẩm, danh mục, seller, lịch sử giá và chỉ số sản phẩm.
- **Analytics layer**: tạo dimensions, snapshot facts và view đọc nhanh cho
  dashboard.

Analytics schema dùng **snapshot-based star schema**. Dự án không giả định crawl
hằng ngày, nên không tạo bảng `daily`; lịch sử giá và metric được lưu theo từng
snapshot crawl/normalize.

## Airflow Và Vận Hành

Airflow điều phối hai DAG chính:

| DAG | Vai trò |
| --- | --- |
| `tiki_crawl_fast` | Crawl danh mục, danh sách sản phẩm, detail sản phẩm, transform và load vào core tables |
| `tiki_analytics_normalize` | Tạo hoặc refresh lớp analytics sau khi core data cập nhật |

`tiki_crawl_fast` ưu tiên ghi payload lớn ra file và chỉ truyền metadata qua
XCom. Cách này giúp tránh phình Airflow metadata database khi số lượng sản phẩm
lớn. Redis dùng cho cache crawl; PostgreSQL là nơi lưu dữ liệu chính.

## Dữ Liệu Đầu Ra

Với người phân tích hoặc người xem báo cáo, các bảng đáng quan tâm là dữ liệu
nghiệp vụ:

- `products`: sản phẩm, giá hiện tại, rating, review, lượng bán, seller và
  category.
- `categories`: danh mục và đường dẫn danh mục.
- `sellers`: nhà bán và trạng thái official/marketplace.
- `product_prices`: lịch sử giá theo thời điểm crawl.
- `product_metrics`: lịch sử rating, review và sold theo thời điểm crawl.
- `analytics.product_analysis_ready`: view đã join sẵn cho Power BI/SQL khi
  analytics schema đã được tạo.

Các bảng như `crawl_runs`, `crawl_tasks`, `raw_products` và `crawl_history` hữu
ích cho vận hành và audit, nhưng không phải đầu ra chính cho báo cáo.

Ví dụ nhãn dùng trực tiếp trong dashboard: `Giá TB`, `KM cao`, `Rating 0`,
`0 review`, `Sold TB`, `Giá tốt`.

## Công Nghệ

- Python 3.11, Apache Airflow, PostgreSQL 16, Redis 7.2, Docker Compose
- requests/aiohttp, BeautifulSoup/lxml
- psycopg2, pandas/numpy
- pytest, Ruff, mypy

## Tài Liệu Liên Quan

- [docs/INDEX.md](docs/INDEX.md): tài liệu kỹ thuật tổng quan.
- [docs/01-PARAMETERS/README.md](docs/01-PARAMETERS/README.md): tham số cấu hình.
- [src/pipelines/crawl/README.md](src/pipelines/crawl/README.md): crawl module.
- [tests/README.md](tests/README.md): hướng dẫn test.
- `graduation_docs/`: tài liệu đồ án, data dictionary và query dashboard.

## Ghi Chú Vận Hành

Các lệnh chạy local, CI, cấu hình và cleanup nằm trong `docs/` và `scripts/`.
Runtime artifacts như `data/`, `exports/`, `backups/`, Airflow logs, cache và
Docker volumes không phải là mã nguồn. Không dùng `docker compose down -v` nếu
muốn giữ dữ liệu PostgreSQL và Redis cache.
