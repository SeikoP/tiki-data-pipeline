# Tài liệu trường dữ liệu sản phẩm (products_final.json)

Tệp này mô tả đầy đủ các trường có trong bản ghi sản phẩm được xuất ở `data/processed/products_final.json` (sau bước Transform). Các trường được chuẩn hóa, làm phẳng (flatten) từ dữ liệu crawl, kèm thêm các trường tính toán (computed fields).

- Nguồn logic: `src/pipelines/transform/transformer.py` (lớp `DataTransformer`).
- Mục tiêu: Thống nhất schema để dùng cho Load (PostgreSQL) và phân tích.

## Tổng quan
- Mỗi dòng (bản ghi) là một sản phẩm đã được chuẩn hóa.
- Kiểu dữ liệu: JSON (UTF-8).
- Một số trường con dạng JSON (giữ nguyên để dễ mở rộng), còn lại được làm phẳng.

## Nhóm trường và mô tả

### 1) Nhận dạng & liên kết
- `product_id` (string): Mã sản phẩm (chỉ gồm số, bắt buộc). Ví dụ: "123456".
- `name` (string): Tên sản phẩm (đã chuẩn hóa khoảng trắng). 
- `url` (string): URL trang sản phẩm (định dạng http/https hợp lệ).
- `image_url` (string | null): Ảnh đại diện sản phẩm (nếu có).

### 2) Danh mục
- `category_url` (string | null): URL danh mục gốc nơi phát hiện sản phẩm.
- `category_id` (string | null): Mã danh mục (extract từ `category_url` nếu có thể).
- `category_path` (array<string> | null): Chuỗi breadcrumb danh mục nếu có.

### 3) Giá & khuyến mãi
- `price` (number | null): Giá hiện tại (đơn vị: VND, >= 0).
- `original_price` (number | null): Giá gốc (VND, >= 0, và `price` <= `original_price`).
- `discount_percent` (number | null): % giảm giá (0–100, làm tròn về số nguyên gần nhất nếu có đủ dữ liệu).

Cách chuẩn hóa:
- Nếu có `price.current_price` và `price.original_price` từ dữ liệu gốc, sẽ tính lại `discount_percent` = `(original_price - price) / original_price * 100` (làm tròn 2 chữ số khi normalize, sau đó khi flatten chuyển về số nguyên gần nhất nếu tồn tại).

### 4) Xếp hạng & đánh giá
- `rating_average` (number | null): Điểm rating trung bình (0–5).
- `review_count` (number | null): Tổng số đánh giá (>= 0).

### 5) Bán hàng
- `sales_count` (number | null): Số lượng đã bán (>= 0). Giá trị được chuẩn hóa từ nhiều key như `sales_count`, `quantity_sold`, `sold`, v.v.

### 6) Người bán
- `seller_name` (string | null): Tên nhà bán/seller.
- `seller_id` (string | null): ID của seller (string hóa).
- `seller_is_official` (boolean): Hàng chính hãng hay không (mặc định `false`).

### 7) Thương hiệu
- `brand` (string | null): Tên thương hiệu (loại bỏ tiền tố “Thương hiệu: ” nếu có).

### 8) Tồn kho
- `stock_available` (boolean | null): Có sẵn hàng hay không.
- `stock_quantity` (number | null): Số lượng tồn kho (nếu có).
- `stock_status` (string | null): Trạng thái tồn kho (nếu có).

### 9) Vận chuyển (JSON giữ nguyên)
- `shipping` (object | null): Thông tin vận chuyển (giữ dạng JSONB để mở rộng). Ví dụ: phí, tốc độ giao, kho, v.v.

### 10) Thông số kỹ thuật & hình ảnh (JSON giữ nguyên)
- `specifications` (object | null): Thông số kỹ thuật/thuộc tính (JSONB).
- `images` (array<object> | null): Danh sách ảnh (JSONB).

### 11) Mô tả
- `description` (string | null): Mô tả sản phẩm (nếu có).

### 12) Thời gian crawl
- `crawled_at` (string | null): Thời gian crawl ở dạng chuỗi ISO-8601 nếu parse được (ví dụ: "2025-11-18T11:05:00"). Nếu không parse được, giữ dạng chuỗi gốc.

### 13) Metadata (chỉ tham chiếu)
- `_metadata` (object | null): Siêu dữ liệu nội bộ nếu được gắn (không dùng để load DB, chủ yếu để debug/trace).

## Trường tính toán (Computed Fields)
Các trường sau được tính trong `DataTransformer._add_computed_fields()`:

- `estimated_revenue` (number | null): Doanh thu ước tính = `sales_count * price` (làm tròn 2 chữ số). Chỉ có khi cả hai đều có giá trị hợp lệ.
- `price_savings` (number | null): Số tiền tiết kiệm = `original_price - price` (nếu `original_price > price`).
- `price_category` (string | null): Nhãn phân khúc giá theo ngưỡng VND:
  - `< 500.000` → "budget"
  - `500.000 – < 2.000.000` → "mid-range"
  - `2.000.000 – < 10.000.000` → "premium"
  - `>= 10.000.000` → "luxury"
- `popularity_score` (number | null): Điểm độ phổ biến (0–100, làm tròn 2 chữ số) kết hợp trọng số:
  - Sales count 50%: `min((sales_count / 100000) * 50, 50)`
  - Rating 30%: `(rating_average / 5) * 30`
  - Review 20%: `min((review_count / 10000) * 20, 20)`
  - Tổng điểm = cộng ba thành phần trên; nếu không có dữ liệu nào → `null`.
- `value_score` (number | null): Điểm “giá trị”; công thức = `rating_average / (price / 1_000_000)` nếu `price > 0` (làm tròn 2 chữ số). Rating cao và giá thấp → điểm cao.
- `discount_amount` (number | null): Số tiền giảm thực tế. Nếu chưa có `price_savings` nhưng có `original_price > price`, dùng `original_price - price`; nếu đã có, đồng nhất với `price_savings`.
- `sales_velocity` (number | null): Tốc độ bán. Hiện tại gán bằng `sales_count` (có thể tinh chỉnh theo trục thời gian nếu có dữ liệu lịch sử).

## Chuẩn hóa dữ liệu (Normalize)
- Chuẩn hóa khoảng trắng trong `name`, `seller_name`.
- Ép kiểu số: `price`, `original_price`, `rating_average`, `review_count`, `sales_count`.
- `product_id`, `seller_id` chuyển thành chuỗi (string).
- URL được trim; (ghi chú: bước canonical hóa URL phục vụ cache nằm trong crawler/cache, không thay đổi format ở transformer trừ khi cần thiết).
- `discount_percent` được tính lại khi có đủ dữ liệu giá.

## Kiểm tra hợp lệ (Validation)
- `product_id`: bắt buộc, là chuỗi chỉ gồm số.
- `name`: bắt buộc, không rỗng.
- `url`: bắt buộc, phải bắt đầu bằng `http://` hoặc `https://`.
- Giá: nếu cả `price` và `original_price` đều tồn tại thì phải `>= 0`, và `price <= original_price`.
- `rating_average`: nếu có, thuộc [0, 5].
- `sales_count`: nếu có, `>= 0`.

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

## Lưu ý khi dùng
- Các trường JSONB (`shipping`, `specifications`, `images`) có thể thay đổi cấu trúc tùy theo nguồn; nên truy vấn có kiểm tra tồn tại key.
- Các trường tính toán phụ thuộc dữ liệu đầu vào; nếu thiếu hoặc không hợp lệ, giá trị sẽ là `null`.
- `popularity_score` và `value_score` chỉ là thước đo heuristic để so sánh tương đối, không phải chỉ số kinh doanh chính thức.

---
Tệp này được sinh thủ công dựa trên logic hiện tại của transformer. Nếu thay đổi schema/logic, vui lòng cập nhật tài liệu này tương ứng.
