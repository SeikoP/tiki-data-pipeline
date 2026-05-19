# Analytics Schema Và Data Dictionary

Tài liệu này mô tả schema analytics sau khi normalize. Mục tiêu của schema là
tạo **dữ liệu chuẩn để phân tích sản phẩm hiện tại**, không phải theo dõi vận
hành crawl, lịch sử giá, hay snapshot theo ngày.

## 1. Nguyên Tắc Thiết Kế

Pipeline có ba lớp:

```text
Raw crawl data
-> Core normalized tables
-> analytics.product_analysis_ready
-> Power BI / báo cáo / slide
```

Trong đó:

- Raw data và core tables giữ dữ liệu vận hành.
- Analytics schema chỉ giữ dữ liệu đã đủ sạch để phân tích.
- Không tạo bảng `daily` vì lịch crawl không cố định theo ngày.
- Không tạo bảng tracking giá vì chưa đảm bảo mỗi lần thay đổi giá đều được
  ghi nhận đầy đủ.
- Không đưa crawl quality/status vào analytics vì đó là monitoring vận hành,
  đã có ở Airflow, log, `crawl_runs`, `crawl_tasks`.
- Không tạo seller giả `unknown`. Sản phẩm thiếu seller hợp lệ bị loại khỏi
  bảng phân tích sạch.
- Không tạo cột `*_label`. Các cột phân loại chứa giá trị tiếng Việt trực tiếp.

## 2. Bảng Analytics Chính

Hiện tại analytics schema chỉ có bảng:

| Bảng | Grain | Mục đích |
| --- | --- | --- |
| `analytics.product_analysis_ready` | 1 dòng cho 1 sản phẩm hiện tại đủ điều kiện phân tích | Bảng chính cho Power BI và SQL phân tích |

Không có:

- `dim_category`, `dim_product`, `dim_seller`
- `fact_product_daily`, `fact_category_daily`, `fact_seller_daily`
- `fact_price_change`
- `fact_crawl_quality`
- `category_summary`, `seller_summary`
- `mart_*`

Các summary như phân tích theo category/seller/segment sẽ được tính bằng
`GROUP BY` trực tiếp trên `product_analysis_ready`. Chỉ tạo summary table sau
này nếu dashboard chậm hoặc có nhu cầu báo cáo cố định.

## 3. Data Dictionary

### Nhóm sản phẩm

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `product_id` | varchar | Mã sản phẩm Tiki, khóa chính |
| `product_name` | varchar | Tên sản phẩm đầy đủ |
| `short_name` | varchar | Tên rút gọn phục vụ chart/table |
| `brand` | varchar | Thương hiệu nếu Tiki/API có dữ liệu |
| `product_url` | text | Link sản phẩm để đối chiếu |
| `image_url` | text | Link ảnh sản phẩm |

### Nhóm category

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `category_id` | varchar | Mã category |
| `category_name` | varchar | Tên category |
| `category_path` | text | Đường dẫn category dạng `level_1 > level_2 > ...` |
| `root_category_name` | varchar | Category gốc |
| `market_scope` | varchar | Phạm vi thị trường tiếng Việt: `Nhà cửa`, `Điện GD`, `Khác` |
| `home_segment` | varchar | Nhóm phân tích tiếng Việt: `Bếp`, `Vệ sinh`, `Giặt ủi`, `Lưu trữ`, `Nhà tắm`, `Tiện nghi`, `Trang trí` |

Sản phẩm có `home_segment = Ngoài scope` không được đưa vào bảng phân tích.

### Nhóm seller

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `seller_id` | varchar | Mã seller hợp lệ |
| `seller_name` | varchar | Tên seller hợp lệ |
| `seller_type` | varchar | `Official` hoặc `Marketplace` |

Không có dòng seller `unknown`. Nếu thiếu cả mã hoặc tên seller, sản phẩm bị
loại khỏi `product_analysis_ready`.

### Nhóm metric

| Cột | Kiểu | Ý nghĩa |
| --- | --- | --- |
| `price` | numeric | Giá bán hiện tại, bắt buộc lớn hơn 0 |
| `original_price` | numeric | Giá gốc nếu hợp lệ và lớn hơn hoặc bằng `price` |
| `discount_percent` | numeric | Phần trăm giảm giá nếu có |
| `rating_average` | numeric | Rating trung bình |
| `review_count` | integer | Số lượt review |
| `sales_count` | integer | Số lượng đã bán |
| `value_score` | numeric | Điểm heuristic kết hợp rating, review và discount |
| `normalized_at` | timestamp | Thời điểm dữ liệu sản phẩm được normalize/crawl gần nhất |

### Nhóm bucket phân tích

Các cột bucket chứa tiếng Việt trực tiếp, dùng ngay cho slicer/legend:

| Cột | Giá trị |
| --- | --- |
| `price_bucket` | `Giá thấp`, `Giá TB`, `Giá cao`, `Cao cấp` |
| `discount_bucket` | `Không KM`, `KM thấp`, `KM TB`, `KM cao` |
| `rating_bucket` | `Thiếu rating`, `Rating 0`, `Đánh giá thấp`, `Đánh giá TB`, `Đánh giá cao` |
| `review_bucket` | `Thiếu review`, `0 review`, `Ít review`, `Review TB`, `Nhiều review` |
| `sold_bucket` | `Thiếu sold`, `0 sold`, `Sold thấp`, `Sold TB`, `Sold cao` |
| `market_position` | `Thiếu tín hiệu`, `Giá tốt`, `Cao cấp tốt`, `KM mạnh`, `Tin cậy thấp`, `Bình thường` |

## 4. Điều Kiện Một Sản Phẩm Được Đưa Vào Analytics

Một sản phẩm chỉ được load vào `product_analysis_ready` khi:

- Có `product_id`.
- Có `product_name`.
- Có `product_url`.
- Có `price > 0`.
- Join được category hợp lệ.
- Category nằm trong segment phân tích chính.
- Có `seller_id`.
- Có `seller_name`.

Quy tắc này cố tình loại dữ liệu thiếu khóa phân tích chính để dashboard không
bị nhiễu bởi dòng `Unknown`, `NULL`, hoặc dữ liệu ngoài phạm vi.

## 5. Vì Sao Không Dùng Star Schema Trong Analytics Layer Này?

Star schema hữu ích khi hệ thống cần nhiều fact, nhiều dimension và lịch sử
ổn định. Với đồ án hiện tại, yêu cầu là phân tích sản phẩm sau một lần normalize
mới nhất, nên một bảng phẳng sạch sẽ phù hợp hơn:

- Dễ dùng trực tiếp trong Power BI.
- Không cần join nhiều bảng.
- Tránh hiểu nhầm rằng dữ liệu đang được crawl hằng ngày.
- Tránh tạo bảng vận hành như `fact_crawl_quality` trong lớp analytics.
- Giảm số cột và tránh cặp cột mã/label trùng lặp.

Nếu sau này pipeline chạy định kỳ ổn định và cần phân tích xu hướng, có thể tạo
schema lịch sử riêng với grain theo `crawl_run`, không dùng tên `daily` nếu không
đảm bảo crawl mỗi ngày.
