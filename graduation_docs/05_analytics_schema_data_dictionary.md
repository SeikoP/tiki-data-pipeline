# Analytics Schema Và Data Dictionary

Schema analytics hiện dùng mô hình **star schema theo snapshot**, không dùng
`daily`. Mục tiêu là có thiết kế dữ liệu đủ chuẩn để trình bày trong đồ án, đồng
thời vẫn phản ánh đúng thực tế: pipeline không cam kết crawl mỗi ngày.

## 1. Luồng Dữ Liệu

```text
Raw crawl data
-> Core normalized tables
-> Analytics dimensions + snapshot facts
-> analytics.product_analysis_ready view
-> Power BI / SQL analysis / báo cáo
```

Analytics không chứa crawl quality hoặc trạng thái DAG. Các dữ liệu đó thuộc
monitoring vận hành, không phải mô hình phân tích sản phẩm.

## 2. Bảng Trong Analytics Schema

| Bảng/View | Grain | Vai trò |
| --- | --- | --- |
| `dim_snapshot` | 1 dòng cho 1 lần crawl/normalize snapshot | Mốc thời gian phân tích |
| `dim_category` | 1 dòng cho 1 category hợp lệ | Taxonomy và segment phân tích |
| `dim_seller` | 1 dòng cho 1 seller hợp lệ | Nhà bán và loại seller |
| `dim_product` | 1 dòng cho 1 sản phẩm hợp lệ | Thuộc tính mô tả sản phẩm |
| `fact_product_snapshot` | 1 sản phẩm tại 1 snapshot | Rating, review, sold, value score |
| `fact_price_snapshot` | 1 sản phẩm tại 1 snapshot | Giá, giá gốc, discount, bucket giá |
| `product_analysis_ready` | View join fact + dimensions | Bảng đọc nhanh cho Power BI/SQL |

Không dùng:

- `fact_*_daily`: vì không đảm bảo crawl hằng ngày.
- `fact_crawl_quality`: vì đó là vận hành.
- `fact_price_change`: chưa lưu cứng; chênh lệch giá có thể tính bằng `LAG()`
  từ `fact_price_snapshot`.
- `category_summary`, `seller_summary`: aggregate có thể tính bằng `GROUP BY`.
- Seller `unknown`: sản phẩm thiếu seller hợp lệ bị loại khỏi model phân tích.

## 3. Dimensions

### `dim_snapshot`

| Cột | Ý nghĩa |
| --- | --- |
| `snapshot_id` | Khóa surrogate của snapshot |
| `snapshot_at` | Thời điểm đại diện của dữ liệu core hiện tại |
| `source` | Nguồn snapshot, hiện là `core_current` |
| `crawl_run_id` | Dự phòng nếu truyền được run id |
| `created_at` | Thời điểm tạo snapshot row |

### `dim_category`

| Cột | Ý nghĩa |
| --- | --- |
| `category_id` | Mã category |
| `category_name` | Tên category |
| `category_path` | Đường dẫn category dạng `level_1 > level_2 > ...` |
| `root_category_name` | Category gốc |
| `market_scope` | `Nhà cửa`, `Điện GD`, `Khác` |
| `home_segment` | `Bếp`, `Vệ sinh`, `Giặt ủi`, `Lưu trữ`, `Nhà tắm`, `Tiện nghi`, `Trang trí`, `Ngoài scope` |
| `is_analysis_target` | Category có thuộc phạm vi phân tích chính không |
| `exclusion_reason` | Lý do ngoài scope nếu có |

### `dim_seller`

| Cột | Ý nghĩa |
| --- | --- |
| `seller_id` | Mã seller |
| `seller_name` | Tên seller |
| `seller_type` | `Official` hoặc `Marketplace` |

### `dim_product`

| Cột | Ý nghĩa |
| --- | --- |
| `product_id` | Mã sản phẩm |
| `product_name` | Tên sản phẩm |
| `short_name` | Tên rút gọn |
| `brand` | Thương hiệu |
| `category_id` | FK tới `dim_category` |
| `seller_id` | FK tới `dim_seller` |
| `product_url` | Link sản phẩm |
| `image_url` | Link ảnh |

## 4. Facts

### `fact_product_snapshot`

| Cột | Ý nghĩa |
| --- | --- |
| `snapshot_id` | FK tới `dim_snapshot` |
| `product_id` | FK tới `dim_product` |
| `category_id` | FK tới `dim_category` |
| `seller_id` | FK tới `dim_seller` |
| `rating_average` | Rating trung bình |
| `rating_bucket` | Bucket rating tiếng Việt |
| `review_count` | Số review |
| `review_bucket` | Bucket review tiếng Việt |
| `sales_count` | Số lượng đã bán |
| `sold_bucket` | Bucket sold tiếng Việt |
| `value_score` | Điểm heuristic |
| `market_position` | Vị trí thị trường |
| `normalized_at` | Thời điểm dữ liệu sản phẩm được ghi nhận |

Khóa chính: `(snapshot_id, product_id)`.

### `fact_price_snapshot`

| Cột | Ý nghĩa |
| --- | --- |
| `snapshot_id` | FK tới `dim_snapshot` |
| `product_id` | FK tới `dim_product` |
| `price` | Giá bán, bắt buộc lớn hơn 0 |
| `original_price` | Giá gốc nếu hợp lệ |
| `discount_percent` | Phần trăm giảm giá |
| `price_bucket` | `Giá thấp`, `Giá TB`, `Giá cao`, `Cao cấp` |
| `discount_bucket` | `Không KM`, `KM thấp`, `KM TB`, `KM cao` |
| `normalized_at` | Thời điểm dữ liệu giá được ghi nhận |

Khóa chính: `(snapshot_id, product_id)`.

Giá thay đổi được tính khi cần:

```sql
SELECT
    product_id,
    snapshot_id,
    price,
    LAG(price) OVER (PARTITION BY product_id ORDER BY snapshot_id) AS previous_price,
    price - LAG(price) OVER (PARTITION BY product_id ORDER BY snapshot_id) AS price_change
FROM analytics.fact_price_snapshot;
```

## 5. View `product_analysis_ready`

`product_analysis_ready` là view join sẵn:

```text
dim_snapshot
fact_product_snapshot
fact_price_snapshot
dim_product
dim_category
dim_seller
```

View này giúp Power BI hoặc SQL đọc nhanh mà không phá mô hình chuẩn hóa. Khi
bảo vệ, trình bày schema chính là dimensions + facts; view chỉ là lớp tiện ích.
