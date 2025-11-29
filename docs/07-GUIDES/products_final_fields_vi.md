# Tài liệu chi tiết trường dữ liệu sản phẩm (`products_final.json`)

Tài liệu này mô tả đầy đủ, chi tiết và có thể áp dụng ngay về schema bản ghi sản phẩm sau bước Transform, được ghi tại `data/processed/products_final.json`. Dữ liệu đã được chuẩn hóa (normalize), làm phẳng (flatten) các trường quan trọng, và bổ sung các trường tính toán (computed fields) phục vụ phân tích.

- Nguồn logic: `src/pipelines/transform/transformer.py` (`DataTransformer`).
- Mục tiêu: Schema thống nhất để Load (PostgreSQL) và phân tích BI.
- Khuyến nghị: Luôn kiểm tra tính hợp lệ trước khi Load.

## Tổng quan
- Mỗi dòng (bản ghi) là một sản phẩm đã được chuẩn hóa.
- Kiểu dữ liệu: JSON (UTF-8), 1 file chứa nhiều bản ghi.
- Nhóm trường JSONB giữ nguyên cấu trúc để dễ mở rộng; các trường trọng yếu đã flatten.
- Nullability được mô tả chi tiết bên dưới để thuận tiện mapping sang PostgreSQL.

## Nhóm trường và mô tả

### 1) Nhận dạng & liên kết
- `product_id` (string, REQUIRED, digits-only): Mã sản phẩm. Ví dụ: "123456".
  - Ràng buộc: chuỗi số, độ dài 1–24, không khoảng trắng.
- `name` (string, REQUIRED): Tên sản phẩm (trim + chuẩn hóa khoảng trắng).
- `url` (string, REQUIRED): URL trang sản phẩm. Ràng buộc: bắt đầu bằng `http://` hoặc `https://`.
- `image_url` (string | null): Ảnh đại diện (nếu có). Ràng buộc: là URL hợp lệ nếu tồn tại.

### 2) Danh mục
- `category_url` (string | null): URL danh mục nơi phát hiện sản phẩm.
- `category_id` (string | null): Mã danh mục (extract từ `category_url` nếu có thể).
- `category_path` (array<string> | null): Chuỗi breadcrumb danh mục nếu có.
  - Chuẩn: tối đa `MAX_CATEGORY_LEVELS = 5` cấp. Nếu vượt quá → truncate về 5.
  - Thành phần: mỗi phần tử là tên cấp danh mục (Title Case, không rỗng).

### 3) Giá & khuyến mãi
- `price` (number | null): Giá hiện tại (VND, `>= 0`).
- `original_price` (number | null): Giá gốc (VND, `>= 0` và `price <= original_price`).
- `discount_percent` (number | null): % giảm (0–100). Làm tròn đến số nguyên gần nhất nếu dữ liệu đủ.

Chuẩn hóa và công thức:
- Nếu có `price` và `original_price`, tính $\text{discount\_percent} = \left(\frac{\text{original\_price} - \text{price}}{\text{original\_price}}\right) \times 100$.
- Làm tròn 2 chữ số khi normalize; flatten về số nguyên gần nhất nếu tồn tại.

### 4) Xếp hạng & đánh giá
- `rating_average` (number | null): Điểm rating trung bình (0–5).
- `review_count` (number | null): Tổng số đánh giá (`>= 0`).

### 5) Bán hàng
- `sales_count` (number | null): Số lượng đã bán (`>= 0`).
  - Gộp/chuẩn hóa từ các key nguồn: `sales_count`, `quantity_sold`, `sold`, v.v.

### 6) Người bán
- `seller_name` (string | null): Tên nhà bán/seller (normalize khoảng trắng).
- `seller_id` (string | null): ID seller (string hóa, digits-only nếu có).
- `seller_is_official` (boolean): Hàng chính hãng hay không (mặc định `false`).

### 7) Thương hiệu
- `brand` (string | null): Tên thương hiệu (loại bỏ tiền tố “Thương hiệu: ” nếu có).

### 8) Tồn kho
- `stock_available` (boolean | null): Có sẵn hàng hay không.
- `stock_quantity` (number | null): Số lượng tồn kho (nếu có, `>= 0`).
- `stock_status` (string | null): Trạng thái tồn kho (ví dụ: `in_stock`, `out_of_stock`).

### 9) Vận chuyển (JSONB)
- `shipping` (object | null): Thông tin vận chuyển (giữ dạng JSONB để mở rộng). Ví dụ: phí, tốc độ giao, kho, v.v.

### 10) Thông số kỹ thuật & hình ảnh (JSONB)
- `specifications` (object | null): Thông số kỹ thuật/thuộc tính.
- `images` (array<object> | null): Danh sách ảnh.

### 11) Mô tả
- `description` (string | null): Mô tả sản phẩm (nếu có).

### 12) Thời gian crawl
- `crawled_at` (string | null): Thời gian crawl ở dạng chuỗi ISO-8601 nếu parse được (ví dụ: "2025-11-18T11:05:00"). Nếu không parse được, giữ dạng chuỗi gốc.

### 13) Metadata (chỉ tham chiếu)
- `_metadata` (object | null): Siêu dữ liệu nội bộ nếu được gắn (không dùng để load DB, chủ yếu để debug/trace).

---

## Bảng nullability & mapping sang PostgreSQL

- Trừ trường JSONB, các trường scalar được khuyến nghị map sang kiểu Postgres tương ứng.
- Định hướng schema (tham khảo):

- `product_id`: `TEXT NOT NULL` (unique key tại tầng ứng dụng; DB upsert dùng `product_id`)
- `name`: `TEXT NOT NULL`
- `url`: `TEXT NOT NULL`
- `image_url`: `TEXT NULL`
- `category_url`: `TEXT NULL`
- `category_id`: `TEXT NULL`
- `category_path`: `JSONB NULL` (array of text)
- `price`: `NUMERIC(12,2) NULL`
- `original_price`: `NUMERIC(12,2) NULL`
- `discount_percent`: `INTEGER NULL`
- `rating_average`: `NUMERIC(3,2) NULL`
- `review_count`: `INTEGER NULL`
- `sales_count`: `INTEGER NULL`
- `seller_name`: `TEXT NULL`
- `seller_id`: `TEXT NULL`
- `seller_is_official`: `BOOLEAN NOT NULL DEFAULT FALSE`
- `brand`: `TEXT NULL`
- `stock_available`: `BOOLEAN NULL`
- `stock_quantity`: `INTEGER NULL`
- `stock_status`: `TEXT NULL`
- `shipping`: `JSONB NULL`
- `specifications`: `JSONB NULL`
- `images`: `JSONB NULL`
- `description`: `TEXT NULL`
- `crawled_at`: `TIMESTAMP WITH TIME ZONE NULL`
- Computed fields: xem bên dưới

---

## Trường tính toán (Computed Fields)
Các trường sau được tính trong `DataTransformer._add_computed_fields()`:

- `estimated_revenue` (number | null): Doanh thu ước tính nếu có `sales_count` và `price` hợp lệ.
  - Công thức: $\text{estimated\_revenue} = \text{sales\_count} \times \text{price}$ (làm tròn 2 chữ số).
- `price_savings` (number | null): Số tiền tiết kiệm nếu `original_price > price`.
  - Công thức: $\text{price\_savings} = \text{original\_price} - \text{price}$.
- `discount_amount` (number | null): Đồng nhất với `price_savings` nếu có; nếu không, tính như trên.
- `price_category` (string | null): Nhãn phân khúc giá theo ngưỡng VND:
  - `< 500.000` → "budget"
  - `500.000 – < 2.000.000` → "mid-range"
  - `2.000.000 – < 10.000.000` → "premium"
  - `>= 10.000.000` → "luxury"
- `popularity_score` (number | null): Điểm độ phổ biến (0–100, 2 chữ số).
  - Sales 50%: $\min\big((\text{sales\_count}/100000) \times 50,\ 50\big)$
  - Rating 30%: $(\text{rating\_average}/5) \times 30$
  - Review 20%: $\min\big((\text{review\_count}/10000) \times 20,\ 20\big)$
  - Tổng: cộng ba thành phần; nếu thiếu toàn bộ → `null`.
- `value_score` (number | null): Điểm “giá trị” nếu `price > 0`.
  - Công thức: $\text{value\_score} = \frac{\text{rating\_average}}{\text{price}/1{,}000{,}000}$ (2 chữ số).
- `sales_velocity` (number | null): Hiện gán bằng `sales_count` (có thể tinh chỉnh theo thời gian khi có lịch sử).

Khuyến nghị mapping Postgres:
- `estimated_revenue`: `NUMERIC(14,2)`
- `price_savings`, `discount_amount`: `NUMERIC(12,2)`
- `popularity_score`, `value_score`: `NUMERIC(5,2)`

## Chuẩn hóa dữ liệu (Normalize)
- Chuẩn hóa khoảng trắng trong `name`, `seller_name` (strip, collapse).
- Ép kiểu số: `price`, `original_price`, `rating_average`, `review_count`, `sales_count`.
- `product_id`, `seller_id` chuyển thành chuỗi (string, digits-only nếu có thể).
- URL: trim; canonical hóa URL phục vụ cache nằm ở crawler, transformer giữ format hợp lệ.
- `discount_percent` được tính lại khi có đủ dữ liệu giá.

Kiểm soát độ dài & ký tự:
- Tên, thương hiệu: loại bỏ dấu xuống dòng, ký tự điều khiển; giới hạn 255 ký tự khi load DB.
- Mô tả: có thể dài; khuyến nghị giới hạn 8–16 KB khi load để tối ưu.

## Kiểm tra hợp lệ (Validation)
- `product_id`: bắt buộc, chuỗi chỉ gồm số.
- `name`: bắt buộc, không rỗng sau normalize.
- `url`: bắt buộc, bắt đầu bằng `http://` hoặc `https://`.
- Giá: nếu cả `price` và `original_price` đều tồn tại thì `>= 0` và `price <= original_price`.
- `rating_average`: nếu có, thuộc [0, 5].
- `review_count`, `sales_count`: nếu có, `>= 0`.

Các lỗi thường gặp & cách xử lý:
- Thiếu `product_id` → loại bản ghi hoặc giữ trong `_metadata` để debug.
- `price > original_price` → đặt `discount_percent = null`, tính lại `price_savings` nếu hợp lệ.
- `category_path` dài hơn 5 → truncate về 5 cấp.
- URL không hợp lệ → ghi log, giữ nguyên chuỗi gốc nếu cần trace.

## Ví dụ bản ghi rút gọn
```json
{
  "product_id": "123456",
  "name": "Bàn phím cơ XYZ",
  "url": "https://tiki.vn/p/123456",
  "image_url": "https://.../img.jpg",
  "category_url": "https://tiki.vn/ban-phim",
  "category_id": "123",
  "sales_count": 124,
  "price": 216000,
  "original_price": 237600,
  "discount_percent": 9,
  "rating_average": 4.5,
  "review_count": 320,
  "seller_name": "ABC Store",
  "seller_id": "98765",
  "seller_is_official": false,
  "brand": "XYZ",
  "stock_available": true,
  "stock_quantity": 50,
  "stock_status": "in_stock",
  "shipping": { "fee": 15000, "warehouse": "HCM" },
  "specifications": { "switch": "Gateron Red" },
  "images": [{ "url": "https://.../1.jpg" }],
  "description": "Bàn phím cơ...",
  "estimated_revenue": 2678400.0,
  "price_savings": 21600.0,
  "price_category": "budget",
  "popularity_score": 28.82,
  "value_score": 55.56,
  "discount_amount": 21600.0,
  "sales_velocity": 124,
  "crawled_at": "2025-11-18T11:05:00"
}
```

## Mapping sang Warehouse (tham khảo)
- Dim Category: `dim_category(level_1..level_5)` có thể được điền từ `category_path` (truncate 5 cấp).
- Fact/Product: upsert theo `product_id`; các trường computed hỗ trợ báo cáo.
- Khuyến nghị thêm `source_dag` nếu cần phân biệt nguồn dữ liệu.

## Truy vấn mẫu (PostgreSQL)
- Top 10 sản phẩm giảm giá nhiều nhất:
```sql
SELECT product_id, name, price_savings
FROM products
WHERE price_savings IS NOT NULL
ORDER BY price_savings DESC
LIMIT 10;
```

- Phân khúc giá và score trung bình theo brand:
```sql
SELECT brand, price_category, AVG(popularity_score) AS avg_popularity
FROM products
WHERE brand IS NOT NULL AND price_category IS NOT NULL
GROUP BY brand, price_category
ORDER BY avg_popularity DESC;
```

## Lưu ý khi dùng
- Các trường JSONB (`shipping`, `specifications`, `images`) có thể thay đổi cấu trúc tùy theo nguồn; nên truy vấn có kiểm tra tồn tại key.
- Các trường tính toán phụ thuộc dữ liệu đầu vào; nếu thiếu hoặc không hợp lệ, giá trị sẽ là `null`.
- `popularity_score` và `value_score` chỉ là thước đo heuristic để so sánh tương đối, không phải chỉ số kinh doanh chính thức.

---
Tệp này được sinh thủ công dựa trên logic hiện tại của transformer. Nếu thay đổi schema/logic, vui lòng cập nhật tài liệu này tương ứng.
