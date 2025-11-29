# 📊 Tiki Warehouse Database Schema — Chi Tiết Dữ Liệu (Actual Schema)

## 📑 Mục Lục

1. [Tổng Quan Database](#tổng-quan-database)
2. [Kiến Trúc Star Schema](#kiến-trúc-star-schema)
3. [Bảng Tham Chiếu Toàn Bộ Trường Dữ Liệu — Thực Tế](#bảng-tham-chiếu-toàn-bộ-trường-dữ-liệu--thực-tế)
4. [Liên Hệ Data Flow](#liên-hệ-data-flow)
5. [Cách Thức Tích Hợp Data](#cách-thức-tích-hợp-data)
6. [Ví Dụ Dữ Liệu Thực Tế](#ví-dụ-dữ-liệu-thực-tế)
7. [Phân Tích Dữ Liệu](#phân-tích-dữ-liệu)
8. [SQL DDL — Tạo Bảng](#sql-ddl--tạo-bảng-create-table-statements)
9. [Tóm Tắt](#tóm-tắt)

---

## 🎯 Tổng Quan Database

### Cơ Sở Dữ Liệu: `tiki_warehouse`

- **Loại:** PostgreSQL Dimensional Warehouse (Star Schema)
- **Source:** Từ database `crawl_data` (ETL layer)
- **Mục đích:** 
  - Phân tích dữ liệu sản phẩm Tiki (analytics)
  - BI dashboards, reports, visualization
  - Dữ liệu tham chiếu cho decision support systems
- **Kiến Trúc:** Star Schema (1 Fact table + 6 Dimension tables)

### Quy Trình Tạo

```
┌─────────────────────────────────┐
│  ETL Layer: crawl_data DB       │
│  - products (normalized)         │
│  - categories (reference)        │
│  - sellers (reference)           │
└──────────────┬──────────────────┘
               │
               │ Extract-Transform
               │ (StarSchemaBuilderV2)
               ↓
┌─────────────────────────────────┐
│  Warehouse Layer: tiki_warehouse│
│  Star Schema:                    │
│  ├─ Fact Table                  │
│  │  └─ fact_product_sales       │
│  └─ Dimension Tables            │
│     ├─ dim_product              │
│     ├─ dim_category             │
│     ├─ dim_seller               │
│     ├─ dim_brand                │
│     ├─ dim_date                 │
│     └─ dim_price_segment        │
└─────────────────────────────────┘
               ↓
        BI Tools / Reports
```

---

## 🔗 Kiến Trúc Star Schema

### Fact Table: `fact_product_sales`

**Mục đích:** Lưu trữ sự kiện bán sản phẩm (product sales events)

```
                    Dimension Tables (Lookups)
                            ↑
                            |
    dim_product ← ← ← ← ← → | ← ← ← ← ← dim_category
         |                   |                   |
         |            fact_product_sales        |
         |                   |                   |
    dim_brand ← ← ← ← ← → | ← ← ← ← ← dim_seller
                            |
         dim_date ← ← ← ← ← |
                            |
    dim_price_segment ← ← ← |
```

**Khóa ngoài (Foreign Keys):**
- `product_sk` → `dim_product.product_sk`
- `category_sk` → `dim_category.category_sk`
- `seller_sk` → `dim_seller.seller_sk`
- `brand_sk` → `dim_brand.brand_sk`
- `date_sk` → `dim_date.date_sk`
- `price_segment_sk` → `dim_price_segment.price_segment_sk`

### Dimension Tables

| Dimension | Mục Đích | Keys | Rows |
|-----------|---------|------|------|
| `dim_product` | Sản phẩm | product_id (UNIQUE) | ~10K-100K |
| `dim_category` | Danh mục | category_id (UNIQUE) | ~50-200 |
| `dim_seller` | Người bán | seller_id (UNIQUE) | ~100-500 |
| `dim_brand` | Thương hiệu | brand_name (UNIQUE) | ~200-1K |
| `dim_date` | Thời gian | date_value (UNIQUE) | ~1-10K |
| `dim_price_segment` | Phân khúc giá | segment_name (UNIQUE) | 6 (fixed) |

---

## 📊 Bảng Tham Chiếu Toàn Bộ Trường Dữ Liệu — Thực Tế

### Bảng 1: fact_product_sales — Chi Tiết Tất Cả Trường

| # | Tên Trường | Loại Dữ Liệu | Khóa | Mục Đích | Ví Dụ |
|---|-----------|-------------|------|---------|-------|
| 1 | `fact_id` | SERIAL | PK | Auto-increment ID | 1, 2, 3, ... |
| 2 | `product_sk` | INT | FK | Liên hệ dim_product | 42 |
| 3 | `category_sk` | INT | FK | Liên hệ dim_category | 5 |
| 4 | `seller_sk` | INT | FK | Liên hệ dim_seller | 3 |
| 5 | `brand_sk` | INT | FK | Liên hệ dim_brand | 12 |
| 6 | `date_sk` | INT | FK | Liên hệ dim_date | 18597 |
| 7 | `price_segment_sk` | INT | FK | Liên hệ dim_price_segment | 4 |
| 8 | `price` | NUMERIC(12,2) | - | Giá hiện tại | 12990000.00 |
| 9 | `original_price` | NUMERIC(12,2) | - | Giá gốc | 14990000.00 |
| 10 | `discount_percent` | NUMERIC(5,2) | - | % khuyến mãi | 13.00 |
| 11 | `quantity_sold` | INT | - | Số lượng bán | 5000 |
| 12 | `estimated_revenue` | NUMERIC(15,2) | - | Doanh số ước tính | 60000000000.00 |
| 13 | `estimated_profit` | NUMERIC(15,2) | - | Lợi nhuận ước tính | 6000000000.00 |
| 14 | `average_rating` | NUMERIC(3,1) | - | Điểm trung bình | 4.8 |
| 15 | `rating_count` | INT | - | Số lượng rating | 1250 |
| 16 | `review_count` | INT | - | Số lượng reviews | 850 |

---

### Bảng 2: dim_product — Chi Tiết Tất Cả Trường

| # | Tên Trường | Loại Dữ Liệu | Khóa | Mục Đích | Ví Dụ |
|---|-----------|-------------|------|---------|-------|
| 1 | `product_sk` | SERIAL | PK | Surrogate key | 1, 2, 3, ... |
| 2 | `product_id` | VARCHAR(50) | UNIQUE | ID sản phẩm Tiki | "286020220" |
| 3 | `product_name` | VARCHAR(500) | - | Tên sản phẩm | "Laptop Dell XPS 13" |
| 4 | `brand` | VARCHAR(255) | - | Thương hiệu | "Dell" |
| 5 | `url` | VARCHAR(500) | - | URL sản phẩm | "https://tiki.vn/p/..." |
| 6 | `created_at` | TIMESTAMP | - | Thời gian tạo | "2024-11-30 10:15:30" |

---

### Bảng 3: dim_category — Chi Tiết Tất Cả Trường

| # | Tên Trường | Loại Dữ Liệu | Khóa | Mục Đích | Ví Dụ |
|---|-----------|-------------|------|---------|-------|
| 1 | `category_sk` | SERIAL | PK | Surrogate key | 1, 2, 3, ... |
| 2 | `category_id` | VARCHAR(50) | UNIQUE | ID danh mục | "4160" |
| 3 | `category_path` | JSONB | - | Đường dẫn đầy đủ | `["Điện tử", "Công nghệ", ...]` |
| 4 | `level_1` | VARCHAR(255) | - | Cấp 1 | "Điện tử" |
| 5 | `level_2` | VARCHAR(255) | - | Cấp 2 | "Công nghệ" |
| 6 | `level_3` | VARCHAR(255) | - | Cấp 3 | "Laptop" |
| 7 | `level_4` | VARCHAR(255) | - | Cấp 4 | "Laptop Gaming" |
| 8 | `level_5` | VARCHAR(255) | - | Cấp 5 | NULL (nếu không có) |

**Lưu ý:** Max 5 levels hierarchy (truncate nếu quá)

---

### Bảng 4: dim_seller — Chi Tiết Tất Cả Trường

| # | Tên Trường | Loại Dữ Liệu | Khóa | Mục Đích | Ví Dụ |
|---|-----------|-------------|------|---------|-------|
| 1 | `seller_sk` | SERIAL | PK | Surrogate key | 1, 2, 3, ... |
| 2 | `seller_id` | VARCHAR(50) | UNIQUE | ID người bán | "1" |
| 3 | `seller_name` | VARCHAR(500) | - | Tên người bán | "Tiki Trading" |

---

### Bảng 5: dim_brand — Chi Tiết Tất Cả Trường

| # | Tên Trường | Loại Dữ Liệu | Khóa | Mục Đích | Ví Dụ |
|---|-----------|-------------|------|---------|-------|
| 1 | `brand_sk` | SERIAL | PK | Surrogate key | 1, 2, 3, ... |
| 2 | `brand_name` | VARCHAR(255) | UNIQUE | Tên thương hiệu | "Apple", "Samsung" |

---

### Bảng 6: dim_date — Chi Tiết Tất Cả Trường

| # | Tên Trường | Loại Dữ Liệu | Khóa | Mục Đích | Ví Dụ |
|---|-----------|-------------|------|---------|-------|
| 1 | `date_sk` | SERIAL | PK | Surrogate key | 1, 2, 3, ... |
| 2 | `date_value` | DATE | UNIQUE | Ngày cụ thể | "2024-11-30" |
| 3 | `year` | INT | - | Năm | 2024 |
| 4 | `month` | INT | - | Tháng (1-12) | 11 |
| 5 | `day` | INT | - | Ngày (1-31) | 30 |

**Ví dụ:**
- date_value = "2024-11-30" → year = 2024, month = 11, day = 30

---

### Bảng 7: dim_price_segment — Chi Tiết Tất Cả Trường

| # | Tên Trường | Loại Dữ Liệu | Khóa | Mục Đích | Ví Dụ |
|---|-----------|-------------|------|---------|-------|
| 1 | `price_segment_sk` | SERIAL | PK | Surrogate key | 1, 2, 3, ... |
| 2 | `segment_name` | VARCHAR(100) | UNIQUE | Tên phân khúc | "Cao (1M-5M)" |
| 3 | `min_price` | NUMERIC | - | Giá thấp nhất | 1000000 |
| 4 | `max_price` | NUMERIC | - | Giá cao nhất | 5000000 |

**Price Segments (6 phân khúc cố định):**
1. Chưa cập nhật (NULL, NULL)
2. Rẻ (< 100K)
3. Bình dân (100K-500K)
4. Trung bình (500K-1M)
5. Cao (1M-5M)
6. Cao cấp (> 5M)

---

## 🔗 Liên Hệ Data Flow

### Bảng 1: PRODUCTS Table — Đầy Đủ Thông Tin Trường

#### A. Fields Nhận Dạng (Identity Fields)

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `id` | SERIAL | PK | NO | 1, 2, 3 | Auto-increment ID | NOT NULL |
| `product_id` | VARCHAR(255) | UNIQUE | NO | "286020220" | Khóa duy nhất Tiki | Digits-only, ≥6 ký tự |
| `name` | VARCHAR(1000) | - | NO | "Laptop Dell XPS..." | Tên sản phẩm | NOT NULL, ≤1000 |
| `url` | TEXT | - | NO | "https://tiki.vn/p/..." | URL sản phẩm | Valid HTTP(S) |
| `image_url` | TEXT | - | YES | "https://salt.tikicdn.com/..." | Ảnh đại diện | Valid URL hoặc NULL |

#### B. Fields Danh Mục (Category Fields)

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `category_url` | TEXT | - | NO | "https://tiki.vn/do-dung..." | URL danh mục | URL format |
| `category_id` | VARCHAR(255) | - | YES | "4160", "8233" | ID danh mục (FK) | Digits-only hoặc NULL |
| `category_path` | JSONB | GIN | YES | `["Điện tử", "Công nghệ", ...]` | Đường dẫn hierarchy | Array, max 5 cấp |

#### C. Fields Giá Cả (Price Fields)

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `price` | DECIMAL(12,2) | - | NO | 12990000.00 | Giá hiện tại | ≥ 0, ≤ original_price |
| `original_price` | DECIMAL(12,2) | - | YES | 14990000.00 | Giá gốc trước CK | ≥ price hoặc NULL |
| `discount_percent` | INTEGER | - | YES | 13 | % khuyến mãi | 0-100 hoặc NULL |
| `discount_amount` | DECIMAL(12,2) | - | YES | 2000000.00 | Tiền CK tuyệt đối | ≥ 0 hoặc NULL |
| `price_savings` | DECIMAL(12,2) | - | YES | 2000000.00 | Tiền tiết kiệm | ≥ 0 hoặc NULL |
| `price_category` | VARCHAR(50) | - | YES | "premium" | Phân nhóm giá | IN ('budget', 'mid-range', 'premium', 'luxury') |
| `estimated_revenue` | DECIMAL(15,2) | - | YES | 60000000000.00 | Doanh số ước tính | ≥ 0 hoặc NULL |

#### D. Fields Đánh Giá (Rating Fields)

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `rating_average` | DECIMAL(3,2) | - | YES | 4.8, 4.5 | Điểm trung bình | 0.0-5.0 hoặc NULL |
| `review_count` | INTEGER | - | YES | 1250, 580 | Số lượng reviews | ≥ 0 hoặc NULL |
| `sales_count` | INTEGER | - | YES | 5000, 150 | Số đã bán | ≥ 0 hoặc NULL |
| `sales_velocity` | INTEGER | - | YES | 50 | Tốc độ bán/ngày | ≥ 0 hoặc NULL |
| `popularity_score` | DECIMAL(10,2) | - | YES | 0.85, 0.42 | Chỉ số phổ biến | 0.0-1.0 hoặc NULL |
| `value_score` | DECIMAL(10,2) | - | YES | 0.73, 0.91 | Chỉ số giá trị | 0.0-1.0 hoặc NULL |

#### E. Fields Chi Tiết Sản Phẩm (Detail Fields)

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `brand` | VARCHAR(255) | - | YES | "Apple", "Samsung" | Thương hiệu | String hoặc NULL |
| `description` | TEXT | - | YES | "Laptop mỏng nhẹ..." | Mô tả dài | Text hoặc NULL, ≤10000 |
| `specifications` | JSONB | - | YES | `{"cpu": "i7", ...}` | Đặc tính kỹ thuật | Valid JSON object |
| `images` | JSONB | - | YES | `["url1", "url2", ...]` | Danh sách hình ảnh | Array of URLs |

#### F. Fields Thông Tin Bán Hàng (Seller Fields)

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `seller_name` | VARCHAR(500) | - | YES | "Tiki Trading" | Tên người bán | String hoặc NULL, ≤500 |
| `seller_id` | VARCHAR(255) | - | YES | "1", "12345" | ID người bán (FK) | Digits-only hoặc NULL |
| `seller_is_official` | BOOLEAN | - | YES | true, false | Bán hàng chính hãng? | true/false, DEFAULT FALSE |

#### G. Fields Kho Hàng (Stock Fields)

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `stock_available` | BOOLEAN | - | YES | true, false | Còn hàng? | true/false hoặc NULL |
| `stock_quantity` | INTEGER | - | YES | 125, 450 | Số lượng kho | ≥ 0 hoặc NULL |
| `stock_status` | VARCHAR(50) | - | YES | "in_stock" | Trạng thái kho | IN ('in_stock', 'out_of_stock', 'limited', 'pre_order', 'unknown') |
| `shipping` | JSONB | - | YES | `{"free": true, ...}` | Thông tin vận chuyển | Valid JSON object |

#### H. Fields Timestamp (Audit Fields)

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `crawled_at` | TIMESTAMP | - | NO | "2024-11-30 10:15:30" | Thời gian crawl đầu | UTC, DEFAULT CURRENT_TIMESTAMP |
| `updated_at` | TIMESTAMP | - | NO | "2024-11-30 10:15:30" | Thời gian update cuối | UTC, DEFAULT CURRENT_TIMESTAMP |

---

### Bảng 2: CATEGORIES Table (Reference Table)

#### Cấu Trúc

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `id` | SERIAL | PK | NO | 1, 2, 3 | Auto-increment ID | NOT NULL |
| `category_id` | VARCHAR(255) | UNIQUE | NO | "4160" | ID danh mục Tiki | Digits-only, NOT NULL |
| `name` | VARCHAR(500) | - | NO | "Laptop" | Tên danh mục | NOT NULL, ≤500 |
| `url` | TEXT | - | NO | "https://tiki.vn/..." | URL danh mục | Valid URL |
| `parent_category_id` | VARCHAR(255) | FK | YES | "1234" | ID danh mục cha | Digits-only hoặc NULL |
| `level` | INTEGER | - | YES | 1, 2, 3, 4, 5 | Mức độ hierarchy | 1-5 hoặc NULL |
| `product_count` | INTEGER | - | YES | 250 | Số sản phẩm | ≥ 0 hoặc NULL |
| `created_at` | TIMESTAMP | - | NO | "2024-01-01 00:00:00" | Thời gian tạo | UTC |
| `updated_at` | TIMESTAMP | - | NO | "2024-11-30 10:15:30" | Thời gian cập nhật | UTC |

---

### Bảng 3: SELLERS Table (Reference Table - Optional)

#### Cấu Trúc

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `id` | SERIAL | PK | NO | 1, 2, 3 | Auto-increment ID | NOT NULL |
| `seller_id` | VARCHAR(255) | UNIQUE | NO | "1", "12345" | ID người bán Tiki | Digits-only, NOT NULL |
| `name` | VARCHAR(500) | - | NO | "Tiki Trading" | Tên người bán | NOT NULL, ≤500 |
| `url` | TEXT | - | YES | "https://tiki.vn/..." | URL seller page | Valid URL hoặc NULL |
| `is_official` | BOOLEAN | - | YES | true, false | Chính hãng? | true/false |
| `rating_average` | DECIMAL(3,2) | - | YES | 4.8 | Đánh giá trung bình | 0.0-5.0 hoặc NULL |
| `total_followers` | INTEGER | - | YES | 10000 | Số followers | ≥ 0 hoặc NULL |
| `response_rate` | DECIMAL(5,2) | - | YES | 98.5 | % phản hồi | 0.0-100.0 hoặc NULL |
| `created_at` | TIMESTAMP | - | NO | "2024-01-01" | Ngày tạo | UTC |
| `updated_at` | TIMESTAMP | - | NO | "2024-11-30" | Ngày cập nhật | UTC |

---

### Bảng 4: PRICE_HISTORY Table (Time Series - Optional)

#### Cấu Trúc (Lưu Lịch Sử Giá)

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `id` | SERIAL | PK | NO | 1, 2, 3 | Auto-increment ID | NOT NULL |
| `product_id` | VARCHAR(255) | FK | NO | "286020220" | Liên kết sản phẩm | NOT NULL |
| `price` | DECIMAL(12,2) | - | NO | 12990000.00 | Giá tại thời điểm | NOT NULL |
| `original_price` | DECIMAL(12,2) | - | YES | 14990000.00 | Giá gốc | ≥ price hoặc NULL |
| `discount_percent` | INTEGER | - | YES | 13 | % CK | 0-100 hoặc NULL |
| `sales_count` | INTEGER | - | YES | 5000 | Số bán | ≥ 0 hoặc NULL |
| `rating_average` | DECIMAL(3,2) | - | YES | 4.8 | Đánh giá | 0.0-5.0 hoặc NULL |
| `stock_available` | BOOLEAN | - | YES | true | Còn hàng? | true/false hoặc NULL |
| `recorded_at` | TIMESTAMP | - | NO | "2024-11-30 10:15:30" | Thời gian ghi | UTC |

**Mục đích:** Lưu lịch sử giá để phân tích xu hướng, price elasticity, seasonal patterns

---

### Bảng 5: PRODUCT_REVIEWS Table (Optional - User Reviews)

#### Cấu Trúc

| Tên Trường | Loại Dữ Liệu | Khóa | Nullable | Ví Dụ | Mục Đích | Validation |
|-----------|-------------|------|---------|-------|---------|-----------|
| `id` | SERIAL | PK | NO | 1, 2, 3 | Auto-increment ID | NOT NULL |
| `product_id` | VARCHAR(255) | FK | NO | "286020220" | Liên kết sản phẩm | NOT NULL |
| `review_id` | VARCHAR(255) | UNIQUE | NO | "rev_12345" | ID review từ Tiki | NOT NULL |
| `rating` | INTEGER | - | NO | 5, 4, 3 | Điểm đánh giá | 1-5 |
| `title` | VARCHAR(500) | - | YES | "Sản phẩm tốt!" | Tiêu đề review | ≤500 hoặc NULL |
| `content` | TEXT | - | YES | "Laptop này rất tốt..." | Nội dung review | ≤5000 hoặc NULL |
| `author_name` | VARCHAR(255) | - | YES | "Người Dùng XYZ" | Tên tác giả | ≤255 hoặc NULL |
| `helpful_count` | INTEGER | - | YES | 25 | Số người thấy hữu ích | ≥ 0 hoặc NULL |
| `created_at` | TIMESTAMP | - | NO | "2024-11-25 15:30:00" | Ngày đăng review | UTC |
| `updated_at` | TIMESTAMP | - | YES | "2024-11-27 10:00:00" | Ngày cập nhật | UTC hoặc NULL |

**Mục đích:** Phân tích sentiment, identify common issues, product improvement insights

---

## 🔍 Cấu Trúc Chi Tiết Các Trường

### Phần 1: Thông Tin Cơ Bản

#### **id** (SERIAL, Primary Key)
- **Định nghĩa:** Auto-increment identifier
- **Loại dữ liệu:** Integer
- **Ví dụ:** 1, 2, 3, ...
- **Mục đích:** Khóa chính DB; không dùng cho business logic
- **Nguồn:** Tự động sinh từ sequence

#### **product_id** (VARCHAR(255), UNIQUE)
- **Định nghĩa:** ID duy nhất từ Tiki
- **Loại dữ liệu:** String (chữ số + ký tự đặc biệt)
- **Ví dụ:** "286020220", "123456789"
- **Mục đích:** Khóa duy nhất; dùng cho deduplication, linking
- **Nguồn:** Lấy từ URL sản phẩm hoặc API Tiki (https://tiki.vn/p/{product_id})
- **Validation:** Digits-only, >= 6 ký tự

#### **name** (VARCHAR(1000))
- **Định nghĩa:** Tên sản phẩm
- **Loại dữ liệu:** String
- **Ví dụ:** "Laptop Dell XPS 13 – Core i7 – 16GB RAM – 512GB SSD"
- **Mục đích:** Hiển thị, tìm kiếm
- **Nguồn:** Lấy từ HTML product page
- **Validation:** NOT NULL, length ≤ 1000

#### **url** (TEXT)
- **Định nghĩa:** URL sản phẩm trên Tiki
- **Loại dữ liệu:** String (URL)
- **Ví dụ:** "https://tiki.vn/p/286020220-..."
- **Mục đích:** Link trực tiếp tới trang product
- **Nguồn:** Lấy từ crawl listings
- **Validation:** NOT NULL, valid HTTP(S) URL

#### **image_url** (TEXT)
- **Định nghĩa:** URL hình ảnh đại diện (thumbnail)
- **Loại dữ liệu:** String (URL)
- **Ví dụ:** "https://salt.tikicdn.com/cache/w386/ts/product/..."
- **Mục đích:** Hiển thị ảnh product trên UI
- **Nguồn:** Lấy từ product detail page
- **Validation:** NULL hoặc valid URL

---

### Phần 2: Phân Loại & Danh Mục

#### **category_url** (TEXT)
- **Định nghĩa:** URL danh mục sản phẩm
- **Loại dữ liệu:** String (URL)
- **Ví dụ:** "https://tiki.vn/do-dung-cong-nghe"
- **Mục đích:** Liên hệ product ↔ category
- **Nguồn:** Từ crawl listing (product được tìm trong category nào)
- **Validation:** NOT NULL, URL format

#### **category_id** (VARCHAR(255))
- **Định nghĩa:** ID danh mục từ Tiki
- **Loại dữ liệu:** String
- **Ví dụ:** "4160", "8233"
- **Mục đích:** Khóa ngoài (FK) tới bảng categories
- **Nguồn:** Lấy từ API/HTML response
- **Validation:** Digits-only, NULL nếu không xác định

#### **category_path** (JSONB)
- **Định nghĩa:** Đường dẫn phân loại đầy đủ (hierarchy)
- **Loại dữ liệu:** JSON Array
- **Ví dụ:**
  ```json
  [
    "Điện tử",
    "Công nghệ",
    "Laptop",
    "Laptop Gaming"
  ]
  ```
- **Mục đích:** Phân tích theo cấp danh mục; drill-down analytics
- **Nguồn:** Xây dựng từ breadcrumb crawl hoặc API
- **Validation:** Array, max 5 cấp, length ≤ 100 mỗi level
- **Constraint:** Truncate nếu > 5 levels (trong transform step)

---

### Phần 3: Giá Cả & Khuyến Mãi

#### **price** (DECIMAL(12, 2))
- **Định nghĩa:** Giá hiện tại (giá bán)
- **Loại dữ liệu:** Số thập phân, 2 chữ số thập phân
- **Ví dụ:** 12990000.00 (₫12.99M)
- **Mục đích:** Giá niêm yết; lập báo cáo doanh thu
- **Nguồn:** Lấy từ product detail page
- **Validation:** NOT NULL, ≥ 0
- **Constraint:** price ≤ original_price

#### **original_price** (DECIMAL(12, 2))
- **Định nghĩa:** Giá gốc (trước khuyến mãi)
- **Loại dữ liệu:** Số thập phân, 2 chữ số thập phân
- **Ví dụ:** 14990000.00 (₫14.99M)
- **Mục đích:** Tính toán discount; so sánh giá trị
- **Nguồn:** Lấy từ product detail page (strikethrough price)
- **Validation:** NULL hoặc ≥ price; nếu NULL → original_price = price

#### **discount_percent** (INTEGER)
- **Định nghĩa:** Phần trăm khuyến mãi
- **Loại dữ liệu:** Integer (0-100)
- **Ví dụ:** 13 (13%)
- **Công thức:** `(original_price - price) / original_price * 100`
- **Mục đích:** Phân tích khuyến mãi; tìm products giá tốt
- **Nguồn:** Computed field (transform step)
- **Validation:** 0-100

#### **discount_amount** (DECIMAL(12, 2))
- **Định nghĩa:** Số tiền khuyến mãi tuyệt đối
- **Loại dữ liệu:** Số thập phân
- **Ví dụ:** 2000000.00 (₫2M tiết kiệm)
- **Công thức:** `original_price - price`
- **Mục đích:** Tính toán savings; marketing messaging
- **Nguồn:** Computed field
- **Validation:** ≥ 0

#### **price_savings** (DECIMAL(12, 2))
- **Định nghĩa:** Tiền tiết kiệm (tương tự discount_amount)
- **Loại dữ liệu:** Số thập phân
- **Ví dụ:** 2000000.00
- **Mục đích:** Báo cáo tiết kiệm
- **Nguồn:** Computed field
- **Validation:** ≥ 0

#### **price_category** (VARCHAR(50))
- **Định nghĩa:** Phân nhóm giá
- **Loại dữ liệu:** String category
- **Ví dụ:** "budget" (0-1M), "mid-range" (1-5M), "premium" (5M+)
- **Mục đích:** Phân tích theo tầng giá; segmentation
- **Nguồn:** Computed field (categorize by price range)
- **Validation:** IN ('budget', 'mid-range', 'premium', 'luxury')

---

### Phần 4: Đánh Giá & Bán Hàng

#### **rating_average** (DECIMAL(3, 2))
- **Định nghĩa:** Điểm trung bình từ review
- **Loại dữ liệu:** Số thập phân, 1-2 chữ số thập phân
- **Ví dụ:** 4.5, 4.8, 3.2
- **Mục đích:** Chất lượng sản phẩm; lọc sản phẩm tốt
- **Nguồn:** Lấy từ product detail page
- **Validation:** 0.0-5.0, NULL nếu không có review

#### **review_count** (INTEGER)
- **Định nghĩa:** Số lượng reviews / ratings
- **Loại dữ liệu:** Integer
- **Ví dụ:** 1250, 580
- **Mục đích:** Độ tin cậy rating; phổ biến
- **Nguồn:** Lấy từ product detail page
- **Validation:** ≥ 0

#### **sales_count** (INTEGER)
- **Định nghĩa:** Số lượng đã bán (sales count / followers)
- **Loại dữ liệu:** Integer
- **Ví dụ:** 5000, 150
- **Mục đích:** Phổ biến; tính doanh số; analytics
- **Nguồn:** Lấy từ product detail (often "sold" label)
- **Validation:** ≥ 0, NULL nếu không xác định

#### **sales_velocity** (INTEGER)
- **Định nghĩa:** Tốc độ bán (sales/day estimate)
- **Loại dữ liệu:** Integer
- **Ví dụ:** 50 (50 sales/day)
- **Mục đích:** Phân tích xu hướng; forecast
- **Nguồn:** Computed field (sales_count / days_on_market)
- **Validation:** ≥ 0

#### **popularity_score** (DECIMAL(10, 2))
- **Định nghĩa:** Chỉ số phổ biến (0-1.0 normalized)
- **Loại dữ liệu:** Số thập phân
- **Ví dụ:** 0.85, 0.42
- **Công thức:** `sales_count / max_sales_count`
- **Mục đích:** Ranking sản phẩm; trending
- **Nguồn:** Computed field (transform step)
- **Validation:** 0.0-1.0

#### **value_score** (DECIMAL(10, 2))
- **Định nghĩa:** Chỉ số giá trị (tổng hợp: giá + rating + phổ biến)
- **Loại dữ liệu:** Số thập phân
- **Ví dụ:** 0.73, 0.91
- **Công thức:** `(discount_percent + popularity_score) / 2` (có thể adjust)
- **Mục đích:** Tìm products "value for money"
- **Nguồn:** Computed field
- **Validation:** 0.0-1.0

#### **estimated_revenue** (DECIMAL(15, 2))
- **Định nghĩa:** Doanh số ước tính (price × sales_count)
- **Loại dữ liệu:** Số thập phân
- **Ví dụ:** 60000000000.00 (₫60B ước tính)
- **Công thức:** `price * sales_count`
- **Mục đích:** Phân tích revenue; forecasting
- **Nguồn:** Computed field
- **Validation:** ≥ 0

---

### Phần 5: Thông Tin Sản Phẩm Chi Tiết

#### **brand** (VARCHAR(255))
- **Định nghĩa:** Thương hiệu / hãng sản xuất
- **Loại dữ liệu:** String
- **Ví dụ:** "Apple", "Samsung", "Dell", "Nike"
- **Mục đích:** Phân tích theo thương hiệu; category analytics
- **Nguồn:** Lấy từ product detail page
- **Validation:** NULL hoặc string, length ≤ 255

#### **description** (TEXT)
- **Định nghĩa:** Mô tả sản phẩm dài
- **Loại dữ liệu:** Text (unbounded)
- **Ví dụ:** "Laptop Dell XPS 13 Plus là ultrabook mỏng nhẹ..."
- **Mục đض:** Tìm kiếm full-text; hiển thị chi tiết
- **Nguồn:** Lấy từ product detail page (HTML paragraph)
- **Validation:** NULL hoặc text, length ≤ 10000

#### **specifications** (JSONB)
- **Định nghĩa:** Đặc tính kỹ thuật chi tiết
- **Loại dữ liệu:** JSON Object
- **Ví dụ:**
  ```json
  {
    "cpu": "Intel Core i7-1360P",
    "ram": "16GB LPDDR5",
    "storage": "512GB SSD NVMe",
    "display": "13.4-inch FHD",
    "weight": "1.2kg",
    "battery": "52Wh"
  }
  ```
- **Mục đích:** Tìm kiếm sản phẩm chi tiết; so sánh specs
- **Nguồn:** Lấy từ product detail page (specs table)
- **Validation:** Valid JSON object
- **Lưu ý:** Cấu trúc flexible (không fixed schema)

#### **images** (JSONB)
- **Định nghĩa:** Danh sách hình ảnh sản phẩm
- **Loại dữ liệu:** JSON Array
- **Ví dụ:**
  ```json
  [
    "https://salt.tikicdn.com/cache/..../1.jpg",
    "https://salt.tikicdn.com/cache/..../2.jpg",
    "https://salt.tikicdn.com/cache/..../3.jpg"
  ]
  ```
- **Mục đích:** Hình ảnh chi tiết; slideshow
- **Nguồn:** Lấy từ product detail page (image URLs)
- **Validation:** Array of URLs
- **Lưu ý:** Thường 3-10 ảnh

---

### Phần 6: Thông Tin Bán Hàng & Kho

#### **seller_name** (VARCHAR(500))
- **Định nghĩa:** Tên người bán
- **Loại dữ liệu:** String
- **Ví dụ:** "Tiki Trading", "TechZone Official", "Best Price"
- **Mục đích:** Phân tích bán hàng; brand identification
- **Nguồn:** Lấy từ product detail page (seller info)
- **Validation:** NULL hoặc string, length ≤ 500

#### **seller_id** (VARCHAR(255))
- **Định nghĩa:** ID người bán (từ Tiki)
- **Loại dữ liệu:** String
- **Ví dụ:** "1", "12345"
- **Mục đích:** Khóa ngoài tới bảng sellers
- **Nguồn:** Lấy từ API/HTML
- **Validation:** Digits-only, NULL nếu không xác định

#### **seller_is_official** (BOOLEAN)
- **Định nghĩa:** Có phải seller chính thức không
- **Loại dữ liệu:** Boolean
- **Ví dụ:** true, false
- **Mục đích:** Lọc sản phẩm chính hãng; quality assurance
- **Nguồn:** Lấy từ product detail (badge/flag)
- **Validation:** true/false, default FALSE
- **Lưu ý:** Thường có badge ✓ hoặc "Chính hãng" label

#### **stock_available** (BOOLEAN)
- **Định nghĩa:** Có còn hàng không
- **Loại dữ liệu:** Boolean
- **Ví dụ:** true, false
- **Mục đích:** Lọc sản phẩm còn hàng; inventory management
- **Nguồn:** Lấy từ product detail (stock status)
- **Validation:** true/false

#### **stock_quantity** (INTEGER)
- **Định nghĩa:** Số lượng còn trong kho
- **Loại dữ liệu:** Integer
- **Ví dụ:** 50, 150, 0
- **Mục đích:** Inventory level; alert low stock
- **Nguồn:** Lấy từ product detail (khi available)
- **Validation:** ≥ 0, NULL nếu không công khai

#### **stock_status** (VARCHAR(50))
- **Định nghĩa:** Trạng thái kho
- **Loại dữ liệu:** String enum
- **Ví dụ:** "in_stock", "out_of_stock", "limited", "pre_order"
- **Mục đích:** Phân tích khả dụng; alert
- **Nguồn:** Lấy từ product detail
- **Validation:** IN ('in_stock', 'out_of_stock', 'limited', 'pre_order', 'unknown')

#### **shipping** (JSONB)
- **Định nghĩa:** Thông tin vận chuyển
- **Loại dữ liệu:** JSON Object
- **Ví dụ:**
  ```json
  {
    "free_shipping": true,
    "same_day_delivery": true,
    "provinces_available": ["HN", "HCMC", "..."],
    "shipping_cost": 0,
    "estimated_days": 1
  }
  ```
- **Mục đích:** Phân tích logistics; cost calculation
- **Nguồn:** Lấy từ product detail
- **Validation:** Valid JSON object

---

### Phần 7: Timestamp & Tracking

#### **crawled_at** (TIMESTAMP)
- **Định nghĩa:** Thời gian crawl lần đầu tiên
- **Loại dữ liệu:** Timestamp (UTC)
- **Ví dụ:** 2024-11-30 10:15:30
- **Mục đích:** Tracking lịch sử; retention policy
- **Nguồn:** Tự động từ DB (DEFAULT CURRENT_TIMESTAMP)
- **Validation:** NOT NULL, auto-set

#### **updated_at** (TIMESTAMP)
- **Định nghĩa:** Thời gian update lần cuối
- **Loại dữ liệu:** Timestamp (UTC)
- **Ví dụ:** 2024-11-30 10:15:30
- **Mục đích:** Tracking lần update gần nhất; freshness
- **Nguồn:** Tự động từ DB (DEFAULT CURRENT_TIMESTAMP, update on upsert)
- **Validation:** NOT NULL, auto-update
- **Trigger:** Cập nhật khi bản ghi được insert/update

---

## 🔗 Liên Hệ Data Flow

### 1. Từ Crawl → Database

```
┌────────────────────────────────────────┐
│  Node 2: Crawl Categories              │
│  (product listings from category page) │
└────────────────────────────────────────┘
             ↓ (XCom: [products])
        ┌─────────────┐
        │ products.json (raw, từ Node 3)
        └─────────────┘
             ↓ (Node 4: extract detail URLs)
┌────────────────────────────────────────┐
│  Node 5: Crawl Product Details         │
│  (brand, specs, images from detail pg) │
└────────────────────────────────────────┘
             ↓ (XCom: [details])
        ┌──────────────────────────────────────┐
        │ products_with_detail.json (Node 6)   │
        │ Fields: name, price, brand, specs... │
        └──────────────────────────────────────┘
             ↓ (Node 7: normalize & compute)
        ┌──────────────────────────────────────┐
        │ products_transformed.json             │
        │ + Computed: discount_percent,         │
        │   estimated_revenue, scores...       │
        └──────────────────────────────────────┘
             ↓ (Node 8: batch upsert)
        ┌──────────────────────────────────────┐
        │ PostgreSQL: crawl_data.products       │
        │ Table updated with all fields         │
        └──────────────────────────────────────┘
```

### 2. Field Mapping từ JSON → Database

| JSON Field | DB Column | Transform | Note |
|-----------|-----------|-----------|------|
| `product_id` | `product_id` | String → VARCHAR | Khóa duy nhất |
| `name` | `name` | String → VARCHAR | Tên sản phẩm |
| `url` | `url` | String → TEXT | URL sản phẩm |
| `price` | `price` | String → DECIMAL | Chuyển đổi ₫ sang số |
| `original_price` | `original_price` | String → DECIMAL | Giá gốc |
| `brand` | `brand` | String → VARCHAR | Extract từ specs |
| `rating` | `rating_average` | String → DECIMAL | Điểm trung bình |
| `specs` | `specifications` | Object → JSONB | Lưu as-is |
| `images` | `images` | Array → JSONB | Danh sách URLs |
| `seller` | `seller_name` | String → VARCHAR | Tên bán hàng |
| `category_path` | `category_path` | Array → JSONB | Hierarchy path |
| — | `discount_percent` | **Computed** | (orig - price) / orig * 100 |
| — | `estimated_revenue` | **Computed** | price * sales_count |
| — | `popularity_score` | **Computed** | sales / max_sales |
| — | `crawled_at` | Auto | CURRENT_TIMESTAMP |
| — | `updated_at` | Auto | CURRENT_TIMESTAMP |

### 3. Upsert Strategy

```sql
INSERT INTO products (product_id, name, price, rating_average, ...)
VALUES (..., ..., ..., ...)
ON CONFLICT (product_id)
DO UPDATE SET
  name = EXCLUDED.name,
  price = EXCLUDED.price,
  rating_average = EXCLUDED.rating_average,
  ...,
  updated_at = CURRENT_TIMESTAMP
WHERE products.updated_at < NOW() - INTERVAL '1 hour';
```

**Cơ chế:**
- Nếu `product_id` chưa tồn tại → INSERT
- Nếu `product_id` tồn tại + record cũ hơn 1h → UPDATE (refresh)
- Nếu `product_id` tồn tại + record mới hơn 1h → SKIP (avoid thrashing)

---

## 📥 Cách Thức Tích Hợp Data

### 1. Crawl Stage (Node 2 & 5)

**Input:** Tiki website (product listings + detail pages)

**Output:** JSON files
- `products.json`: Listing info (id, name, price, rating)
- `products_with_detail.json`: + detail info (brand, specs, images)

**Process:**
- Selenium + Requests: Gửi request tới product page
- Parse HTML: Extract text, attributes, JSONB fields
- Cache: Lưu raw response vào `data/raw/products/detail/cache/`
- Validate: Basic schema check
- XCom: Pass product list tới merge task

### 2. Transform Stage (Node 7)

**Input:** `products_with_detail.json`

**Output:** `products_transformed.json`

**Process:**
- **Type conversion:** String → numbers (price, rating)
- **Validation:** 
  - price ≤ original_price?
  - rating ∈ [0, 5]?
  - product_id digits-only?
- **Computation:**
  ```
  discount_percent = (original_price - price) / original_price * 100
  estimated_revenue = price * sales_count
  popularity_score = sales_count / max_sales_count
  value_score = (discount_percent + popularity_score) / 2
  ```
- **Truncation:** category_path nếu > 5 cấp
- **Logging:** Invalid rows → error log (không fail DAG)

### 3. Load Stage (Node 8)

**Input:** `products_transformed.json`

**Output:** PostgreSQL `crawl_data.products` table

**Process:**
- **Batch read:** Chia file thành chunks (500-1000 rows)
- **Upsert:** `ON CONFLICT DO UPDATE`
- **Idempotent:** Run lại không duplicate
- **Transaction:** Mỗi batch trong 1 transaction
- **Rollback:** Nếu constraint violation → skip batch + log
- **Final JSON:** Lưu output list vào `data/processed/products_final.json`

---

## 💡 Ví Dụ Dữ Liệu Thực Tế

### Ví Dụ 1: Laptop (High Value)

```json
{
  "id": 42857,
  "product_id": "286020220",
  "name": "Laptop Dell XPS 13 Plus – Core i7-1360P – 16GB RAM – 512GB SSD",
  "url": "https://tiki.vn/p/286020220-...",
  "image_url": "https://salt.tikicdn.com/cache/w386/ts/product/...",
  "category_url": "https://tiki.vn/do-dung-cong-nghe",
  "category_id": "4160",
  "category_path": ["Điện tử", "Công nghệ", "Laptop", "Laptop Gaming"],
  "price": 32990000.00,
  "original_price": 38990000.00,
  "discount_percent": 15,
  "discount_amount": 6000000.00,
  "price_savings": 6000000.00,
  "price_category": "premium",
  "rating_average": 4.8,
  "review_count": 1250,
  "sales_count": 3500,
  "sales_velocity": 42,
  "popularity_score": 0.92,
  "value_score": 0.84,
  "estimated_revenue": 115465000000.00,
  "brand": "Dell",
  "description": "Dell XPS 13 Plus là ultrabook mỏng nhẹ, hiệu năng mạnh...",
  "specifications": {
    "cpu": "Intel Core i7-1360P",
    "cores": "12-core",
    "ram": "16GB LPDDR5",
    "storage": "512GB SSD NVMe",
    "display": "13.4-inch FHD 1920x1200",
    "weight": "1.2kg",
    "battery": "52Wh",
    "os": "Windows 11"
  },
  "images": [
    "https://salt.tikicdn.com/cache/.../1.jpg",
    "https://salt.tikicdn.com/cache/.../2.jpg",
    "https://salt.tikicdn.com/cache/.../3.jpg"
  ],
  "seller_name": "Tiki Trading",
  "seller_id": "1",
  "seller_is_official": true,
  "stock_available": true,
  "stock_quantity": 125,
  "stock_status": "in_stock",
  "shipping": {
    "free_shipping": true,
    "same_day_delivery": true,
    "provinces_available": ["HN", "HCMC", "..."],
    "shipping_cost": 0,
    "estimated_days": 1
  },
  "crawled_at": "2024-11-30 10:15:30",
  "updated_at": "2024-11-30 10:15:30"
}
```

### Ví Dụ 2: Sản Phẩm Budget (Giá Rẻ)

```json
{
  "id": 12543,
  "product_id": "123456789",
  "name": "Bộ sạc USB Type-C 65W Quick Charge",
  "url": "https://tiki.vn/p/123456789-...",
  "image_url": "https://salt.tikicdn.com/cache/.../charger.jpg",
  "category_url": "https://tiki.vn/phu-kien",
  "category_id": "8233",
  "category_path": ["Điện tử", "Phụ kiện", "Cáp & Sạc"],
  "price": 189000.00,
  "original_price": 249000.00,
  "discount_percent": 24,
  "discount_amount": 60000.00,
  "price_savings": 60000.00,
  "price_category": "budget",
  "rating_average": 4.6,
  "review_count": 3421,
  "sales_count": 18500,
  "sales_velocity": 89,
  "popularity_score": 1.0,
  "value_score": 0.95,
  "estimated_revenue": 3496500000.00,
  "brand": "Baseus",
  "description": "Sạc USB Type-C 65W hỗ trợ Quick Charge 3.0...",
  "specifications": {
    "power": "65W",
    "ports": "1x USB-C",
    "protocol": "Quick Charge 3.0, USB PD",
    "input": "AC 100-240V",
    "color": "Black"
  },
  "images": [
    "https://salt.tikicdn.com/cache/.../1.jpg",
    "https://salt.tikicdn.com/cache/.../2.jpg"
  ],
  "seller_name": "Best Price Electronics",
  "seller_id": "54321",
  "seller_is_official": false,
  "stock_available": true,
  "stock_quantity": 450,
  "stock_status": "in_stock",
  "shipping": {
    "free_shipping": true,
    "same_day_delivery": false,
    "provinces_available": ["HN", "HCMC", "DN", "..."],
    "shipping_cost": 0,
    "estimated_days": 2
  },
  "crawled_at": "2024-11-30 09:45:20",
  "updated_at": "2024-11-30 09:45:20"
}
```

### Ví Dụ 3: Sản Phẩm Hết Hàng

```json
{
  "id": 78901,
  "product_id": "987654321",
  "name": "Gaming Monitor ASUS ROG 240Hz 1ms (Limited Edition)",
  "url": "https://tiki.vn/p/987654321-...",
  "image_url": null,
  "category_url": "https://tiki.vn/man-hinh-may-tinh",
  "category_id": "9876",
  "category_path": ["Điện tử", "Công nghệ", "Màn hình máy tính"],
  "price": 8990000.00,
  "original_price": 9990000.00,
  "discount_percent": 10,
  "discount_amount": 1000000.00,
  "price_savings": 1000000.00,
  "price_category": "premium",
  "rating_average": 4.9,
  "review_count": 850,
  "sales_count": 1200,
  "sales_velocity": null,
  "popularity_score": 0.75,
  "value_score": 0.60,
  "estimated_revenue": 10788000000.00,
  "brand": "ASUS",
  "description": "Màn hình gaming ASUS ROG 240Hz với response time 1ms...",
  "specifications": {
    "size": "27-inch",
    "resolution": "1920x1080",
    "refresh_rate": "240Hz",
    "response_time": "1ms",
    "panel": "IPS"
  },
  "images": [
    "https://salt.tikicdn.com/cache/.../1.jpg"
  ],
  "seller_name": "Tech Kingdom",
  "seller_id": "99999",
  "seller_is_official": true,
  "stock_available": false,
  "stock_quantity": 0,
  "stock_status": "out_of_stock",
  "shipping": {
    "free_shipping": false,
    "same_day_delivery": false,
    "provinces_available": [],
    "shipping_cost": null,
    "estimated_days": null
  },
  "crawled_at": "2024-11-15 14:22:15",
  "updated_at": "2024-11-30 10:05:30"
}
```

---

## 📊 Phân Tích Dữ Liệu

### 1. Các Loại Truy Vấn Phổ Biến

#### A. Top Products by Revenue

```sql
SELECT 
  product_id, 
  name, 
  estimated_revenue, 
  sales_count,
  price
FROM products
WHERE estimated_revenue > 0
ORDER BY estimated_revenue DESC
LIMIT 20;
```

**Output:** Top 20 sản phẩm theo doanh số ước tính

#### B. Products by Price Category & Rating

```sql
SELECT 
  price_category,
  COUNT(*) as product_count,
  AVG(rating_average) as avg_rating,
  AVG(discount_percent) as avg_discount
FROM products
WHERE price_category IN ('budget', 'mid-range', 'premium')
GROUP BY price_category
ORDER BY avg_rating DESC;
```

**Output:** So sánh rating/discount theo tầm giá

#### C. Best Value Products (High Rating + High Discount)

```sql
SELECT 
  product_id,
  name,
  rating_average,
  discount_percent,
  value_score
FROM products
WHERE rating_average >= 4.5
  AND discount_percent >= 15
ORDER BY value_score DESC
LIMIT 50;
```

**Output:** 50 sản phẩm "value for money" tốt nhất

#### D. Category Hierarchy Analysis

```sql
SELECT 
  category_path[1] as level1,
  category_path[2] as level2,
  COUNT(*) as product_count,
  SUM(estimated_revenue) as total_revenue,
  AVG(rating_average) as avg_rating
FROM products
WHERE category_path IS NOT NULL
GROUP BY category_path[1], category_path[2]
ORDER BY total_revenue DESC;
```

**Output:** Phân tích doanh số theo danh mục (2 cấp)

#### E. Stock Status Report

```sql
SELECT 
  stock_status,
  COUNT(*) as count,
  COUNT(CASE WHEN rating_average >= 4.5 THEN 1 END) as high_rated,
  AVG(price) as avg_price
FROM products
GROUP BY stock_status;
```

**Output:** Tỷ lệ hàng còn/hết + rating trung bình

### 2. Các Chỉ Số Chính (KPIs)

| KPI | SQL | Ý Nghĩa |
|-----|-----|---------|
| Total Products | `COUNT(*)` | Tổng số sản phẩm |
| Total Categories | `COUNT(DISTINCT category_id)` | Số danh mục khác nhau |
| Est. Total Revenue | `SUM(estimated_revenue)` | Doanh số ước tính tổng |
| Avg Price | `AVG(price)` | Giá trung bình |
| Avg Rating | `AVG(rating_average)` | Đánh giá trung bình |
| High-Rated % | `COUNT(*) FILTER (WHERE rating >= 4.5) / COUNT(*)` | % sản phẩm rating ≥ 4.5 |
| In-Stock % | `COUNT(*) FILTER (WHERE stock_available) / COUNT(*)` | % hàng còn |
| Avg Discount | `AVG(discount_percent)` | % khuyến mãi trung bình |

### 3. Dữ Liệu Tương Quan

#### Mối quan hệ: Discount ↔ Sales

```sql
SELECT 
  CASE 
    WHEN discount_percent = 0 THEN '0%'
    WHEN discount_percent < 10 THEN '1-10%'
    WHEN discount_percent < 20 THEN '10-20%'
    WHEN discount_percent < 30 THEN '20-30%'
    ELSE '>30%'
  END as discount_range,
  COUNT(*) as product_count,
  AVG(sales_count) as avg_sales,
  AVG(rating_average) as avg_rating
FROM products
WHERE sales_count > 0
GROUP BY discount_range
ORDER BY discount_range;
```

**Insight:** Sản phẩm khuyến mãi lớn có bán được nhiều hơn không?

#### Mối quan hệ: Brand ↔ Rating

```sql
SELECT 
  brand,
  COUNT(*) as product_count,
  AVG(rating_average) as avg_rating,
  AVG(sales_count) as avg_sales,
  SUM(estimated_revenue) as total_revenue
FROM products
WHERE brand IS NOT NULL
GROUP BY brand
HAVING COUNT(*) >= 5
ORDER BY avg_rating DESC
LIMIT 20;
```

**Insight:** Thương hiệu nào có rating cao nhất?

---

## 💾 SQL DDL — Tạo Bảng (CREATE TABLE Statements - ACTUAL WAREHOUSE)

### 1. Bảng DIM_PRICE_SEGMENT — SQL DDL

```sql
CREATE TABLE IF NOT EXISTS dim_price_segment (
    price_segment_sk SERIAL PRIMARY KEY,
    segment_name VARCHAR(100) UNIQUE,
    min_price NUMERIC,
    max_price NUMERIC
);

-- 6 Price Segments được load tự động:
-- 1. Chưa cập nhật (NULL, NULL)
-- 2. Rẻ (< 100K) → (0, 100000)
-- 3. Bình dân (100K-500K) → (100000, 500000)
-- 4. Trung bình (500K-1M) → (500000, 1000000)
-- 5. Cao (1M-5M) → (1000000, 5000000)
-- 6. Cao cấp (> 5M) → (5000000, NULL)
```

---

### 2. Bảng DIM_DATE — SQL DDL

```sql
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk SERIAL PRIMARY KEY,
    date_value DATE UNIQUE,
    year INT,
    month INT,
    day INT
);

-- Index for date lookups
CREATE INDEX IF NOT EXISTS idx_dim_date_value ON dim_date(date_value);
```

---

### 3. Bảng DIM_BRAND — SQL DDL

```sql
CREATE TABLE IF NOT EXISTS dim_brand (
    brand_sk SERIAL PRIMARY KEY,
    brand_name VARCHAR(255) UNIQUE
);

-- Index for brand lookups
CREATE INDEX IF NOT EXISTS idx_dim_brand_name ON dim_brand(brand_name);
```

---

### 4. Bảng DIM_SELLER — SQL DDL

```sql
CREATE TABLE IF NOT EXISTS dim_seller (
    seller_sk SERIAL PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE,
    seller_name VARCHAR(500)
);

-- Index for seller lookups
CREATE INDEX IF NOT EXISTS idx_dim_seller_id ON dim_seller(seller_id);
```

---

### 5. Bảng DIM_CATEGORY — SQL DDL

```sql
CREATE TABLE IF NOT EXISTS dim_category (
    category_sk SERIAL PRIMARY KEY,
    category_id VARCHAR(50) UNIQUE,
    category_path JSONB,
    level_1 VARCHAR(255),
    level_2 VARCHAR(255),
    level_3 VARCHAR(255),
    level_4 VARCHAR(255),
    level_5 VARCHAR(255)
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_dim_category_id ON dim_category(category_id);
CREATE INDEX IF NOT EXISTS idx_dim_category_path ON dim_category USING GIN (category_path);
CREATE INDEX IF NOT EXISTS idx_dim_category_level1 ON dim_category(level_1);
```

---

### 6. Bảng DIM_PRODUCT — SQL DDL

```sql
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk SERIAL PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE,
    product_name VARCHAR(500),
    brand VARCHAR(255),
    url VARCHAR(500),
    created_at TIMESTAMP
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_dim_product_id ON dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_brand ON dim_product(brand);
```

---

### 7. Bảng FACT_PRODUCT_SALES — SQL DDL (Main Fact Table)

```sql
CREATE TABLE IF NOT EXISTS fact_product_sales (
    fact_id SERIAL PRIMARY KEY,
    product_sk INT REFERENCES dim_product(product_sk),
    category_sk INT REFERENCES dim_category(category_sk),
    seller_sk INT REFERENCES dim_seller(seller_sk),
    brand_sk INT REFERENCES dim_brand(brand_sk),
    date_sk INT REFERENCES dim_date(date_sk),
    price_segment_sk INT REFERENCES dim_price_segment(price_segment_sk),
    
    -- Price
    price NUMERIC(12, 2),
    original_price NUMERIC(12, 2),
    discount_percent NUMERIC(5, 2),
    
    -- Sales & Revenue
    quantity_sold INT,
    estimated_revenue NUMERIC(15, 2),
    estimated_profit NUMERIC(15, 2),
    
    -- Rating & Reviews
    average_rating NUMERIC(3, 1),
    rating_count INT,
    review_count INT
);

-- Fact Table Indexes
CREATE INDEX IF NOT EXISTS idx_fact_product_sk ON fact_product_sales(product_sk);
CREATE INDEX IF NOT EXISTS idx_fact_category_sk ON fact_product_sales(category_sk);
CREATE INDEX IF NOT EXISTS idx_fact_seller_sk ON fact_product_sales(seller_sk);
CREATE INDEX IF NOT EXISTS idx_fact_brand_sk ON fact_product_sales(brand_sk);
CREATE INDEX IF NOT EXISTS idx_fact_date_sk ON fact_product_sales(date_sk);
CREATE INDEX IF NOT EXISTS idx_fact_price_segment_sk ON fact_product_sales(price_segment_sk);

-- Composite indexes for common queries
CREATE INDEX IF NOT EXISTS idx_fact_category_date 
ON fact_product_sales(category_sk, date_sk);

CREATE INDEX IF NOT EXISTS idx_fact_product_rating 
ON fact_product_sales(product_sk, average_rating DESC);
```

---

### SQL Views for Common Analysis

```sql
-- View: Top Products by Revenue
CREATE OR REPLACE VIEW vw_top_products_revenue AS
SELECT 
    dp.product_sk,
    dp.product_id,
    dp.product_name,
    db.brand_name,
    ds.seller_name,
    SUM(fp.estimated_revenue) as total_revenue,
    COUNT(*) as record_count,
    AVG(fp.average_rating) as avg_rating,
    SUM(fp.quantity_sold) as total_quantity
FROM fact_product_sales fp
JOIN dim_product dp ON fp.product_sk = dp.product_sk
JOIN dim_brand db ON fp.brand_sk = db.brand_sk
JOIN dim_seller ds ON fp.seller_sk = ds.seller_sk
GROUP BY dp.product_sk, dp.product_id, dp.product_name, db.brand_name, ds.seller_name
ORDER BY total_revenue DESC;

-- View: Category Performance
CREATE OR REPLACE VIEW vw_category_performance AS
SELECT 
    dc.category_sk,
    dc.category_id,
    dc.level_1,
    dc.level_2,
    COUNT(DISTINCT fp.product_sk) as product_count,
    SUM(fp.estimated_revenue) as total_revenue,
    AVG(fp.average_rating) as avg_rating,
    SUM(fp.quantity_sold) as total_quantity
FROM fact_product_sales fp
JOIN dim_category dc ON fp.category_sk = dc.category_sk
GROUP BY dc.category_sk, dc.category_id, dc.level_1, dc.level_2;

-- View: Daily Sales Metrics
CREATE OR REPLACE VIEW vw_daily_sales AS
SELECT 
    dd.date_value,
    dd.year,
    dd.month,
    dd.day,
    COUNT(DISTINCT fp.product_sk) as product_count,
    SUM(fp.estimated_revenue) as daily_revenue,
    AVG(fp.average_rating) as avg_rating
FROM fact_product_sales fp
JOIN dim_date dd ON fp.date_sk = dd.date_sk
GROUP BY dd.date_value, dd.year, dd.month, dd.day;

-- View: Price Segment Analysis
CREATE OR REPLACE VIEW vw_price_segment_analysis AS
SELECT 
    dps.segment_name,
    COUNT(*) as product_count,
    AVG(fp.price) as avg_price,
    SUM(fp.estimated_revenue) as total_revenue,
    AVG(fp.average_rating) as avg_rating,
    AVG(fp.discount_percent) as avg_discount
FROM fact_product_sales fp
JOIN dim_price_segment dps ON fp.price_segment_sk = dps.price_segment_sk
GROUP BY dps.segment_name;
```

---

## 📋 Tóm Tắt

### Database: `tiki_warehouse` (Star Schema)

**Kiến trúc:** Dimensional Data Warehouse (Kimball Star Schema)

**Mục đích:**
- Lưu trữ sản phẩm Tiki cho phân tích (analytics)
- Hỗ trợ BI dashboards, reports, visualization
- Dữ liệu tham chiếu cho decision support systems

**Bảng:**
- **Fact Table:** `fact_product_sales` (các sự kiện bán)
- **Dimension Tables:**
  - `dim_product` (sản phẩm)
  - `dim_category` (danh mục, 5 levels)
  - `dim_seller` (người bán)
  - `dim_brand` (thương hiệu)
  - `dim_date` (thời gian: năm, tháng, ngày)
  - `dim_price_segment` (phân khúc giá: 6 segments)

**Fact Fields:**
- **Price:** price, original_price, discount_percent
- **Sales & Revenue:** quantity_sold, estimated_revenue, estimated_profit
- **Rating & Reviews:** average_rating, rating_count, review_count

**Upsert Strategy:**
- Dimension tables: UNIQUE keys, UPSERT logic
- Fact table: INSERT new records (append-only)

**Optimize:**
- Surrogate keys (SK) trên tất cả dimension
- Foreign keys từ fact → dimensions
- Composite indexes cho common queries
- JSONB category_path cho hierarchical queries

**Data Flow:**
- crawl_data.products → ETL (StarSchemaBuilderV2) → tiki_warehouse (star schema) → BI/Analytics

