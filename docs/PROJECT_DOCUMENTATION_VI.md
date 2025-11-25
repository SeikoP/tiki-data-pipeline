# TÍCH HỢP DỮ LIỆU TIKI BẰNG KIẾN TRÚC STAR SCHEMA
## Dự án Phân Tích Và Trực Quan Hóa Dữ Liệu E-Commerce

**Phiên bản:** 1.0  
**Ngày tạo:** 26 Tháng 11, 2025  
**Trạng thái:** Hoàn Thành Giai Đoạn ETL  

---

## 1. BỐI CẢNH DỰ ÁN

### 1.1 Tổng Quan
Trong bối cảnh thương mại điện tử Việt Nam đang bùng nổ, Tiki.vn là một trong những nền tảng lớn nhất với hàng triệu sản phẩm và giao dịch hàng ngày. Để hiểu rõ hơn về thị trường, hành vi người bán, xu hướng sản phẩm và cơ hội tối ưu hóa, cần có một hệ thống phân tích dữ liệu toàn diện.

Dự án này xây dựng một **Data Warehouse dựa trên kiến trúc Star Schema**, cho phép các nhà phân tích dễ dàng truy vấn, phân tích và visualize dữ liệu sản phẩm Tiki. Hệ thống được thiết kế để hỗ trợ các quyết định kinh doanh từ cấp chiến lược đến cấp tác vụ.

### 1.2 Quy Mô Dữ Liệu Hiện Tại
- **Tổng sản phẩm crawl:** 1,122 sản phẩm (lần đầu tiên)
- **Sản phẩm sau lọc (brand & seller hợp lệ):** 885 sản phẩm (78.9%)
- **Danh mục:** 209 danh mục phân cấp
- **Thương hiệu:** 267 thương hiệu khác nhau
- **Người bán:** 252 seller tích cực
- **Thời gian crawl:** Single snapshot (2025-11-26)

---

## 2. LÝ DO CHỌN ĐỀ TÀI

### 2.1 Các Vấn Đề Thực Tế
1. **Dữ liệu rời rạc:** Tiki cung cấp API giới hạn; dữ liệu phải thu thập qua web scraping
2. **Cấu trúc phức tạp:** Dữ liệu thô chứa nhiều trường lồng nhau (JSON), cần normalization
3. **Chất lượng dữ liệu:** Nhiều sản phẩm thiếu thông tin quan trọng (brand, seller info)
4. **Khó phân tích trực tiếp:** Không thể chạy aggregate queries hiệu quả trên dữ liệu thô
5. **Không có visualization:** Khó tạo dashboard để tracking performance

### 2.2 Giá Trị Mang Lại
- **Tăng tốc độ phân tích:** Star Schema tối ưu cho queries OLAP, truy vấn nhanh hơn 10-100x so với 3NF normalization
- **Dễ hiểu & bảo trì:** Kiến trúc Star đơn giản, người phân tích không cần chuyên gia DB
- **Hỗ trợ BI tools:** Dữ liệu sẵn sàng cho Power BI, Tableau, hoặc các công cụ visualize khác
- **Scalable:** Có thể mở rộng dữ liệu và thêm dimension mới mà không ảnh hưởng query performance
- **Data quality control:** Lọc bỏ dữ liệu không hợp lệ, đảm bảo tính chính xác

---

## 3. MỤC TIÊU NGHIÊN CỨU & MỤC TIÊU DỰ ÁN

### 3.1 Mục Tiêu Chính
1. **Xây dựng Data Warehouse:** Tạo một hệ thống truy vấn dữ liệu tập trung, tối ưu cho phân tích
2. **Chuẩn hóa dữ liệu:** Chuyển từ dữ liệu thô (crawl_data) sang dữ liệu chuẩn hóa (3NF) rồi sang dữ liệu phân tích (Star Schema)
3. **Đảm bảo chất lượng:** Loại bỏ records không đầy đủ (NULL brand, NULL seller)
4. **Tạo nền tảng phân tích:** Sẵn sàng cho dashboard, reporting, và data-driven decisions

### 3.2 Mục Tiêu Cụ Thể
- Có **885 sản phẩm** dữ liệu sạch trong warehouse
- **6 dimension tables** (Product, Category, Seller, Brand, Date, Price Segment) liên kết với **1 fact table** (Product Sales)
- Suport **minimum 10+ loại phân tích** (xem phần kỳ vọng kết quả)
- Performance: Trả lại kết quả trong **< 1 giây** cho bất kỳ query nào

### 3.3 Các Câu Hỏi Kinh Doanh Cần Trả Lời
- Thương hiệu nào có số sản phẩm nhiều nhất? Thương hiệu nào có rating cao nhất?
- Phân khúc giá nào chiếm doanh thu cao nhất?
- Danh mục nào là hottest categories?
- Mối quan hệ giữa giá và rating như thế nào?
- Những sản phẩm nào bán chạy nhất (dựa vào sales velocity)?

---

## 4. ĐỐI TƯỢNG HƯỚNG TỚI

### 4.1 Người Dùng Cuối
1. **Business Analysts & Data Analysts**
   - Chạy queries tùy chỉnh để phân tích trends
   - Tạo ad-hoc reports
   - Tìm kiếm insights để support business decisions

2. **Marketing & Strategy Team**
   - Hiểu thị trường: thương hiệu trending, danh mục hot
   - Identify opportunities: seller tốt, product gaps
   - Campaign planning dựa trên dữ liệu

3. **Operations & Supply Chain**
   - Track product performance theo seller, category
   - Optimize inventory dựa vào sales velocity
   - Monitor supplier quality (rating, review)

4. **Data Engineers & DBAs**
   - Maintain & monitor warehouse performance
   - Implement additional data pipelines
   - Ensure data quality & consistency

### 4.2 Use Cases Chính
- **Executive Dashboard:** Real-time metrics cho leadership
- **Category Performance Report:** Top/bottom performers
- **Seller Quality Scorecard:** Track seller metrics
- **Pricing Analysis:** Discount impact, price elasticity
- **Product Recommendations:** Similar products, trending items

---

## 5. PHẠM VI DỰ ÁN

### 5.1 Phạm Vi Bao Gồm
✅ **Dữ liệu Crawl:**
- Thông tin sản phẩm: tên, URL, mô tả cơ bản
- Giá: giá hiện tại, giá gốc, % discount
- Rating & Review: điểm trung bình, số lượt đánh giá, review count
- Seller info: tên, loại (official/3rd-party)
- Brand & Category: phân loại sản phẩm
- Metadata: crawl timestamp, stock status

✅ **ETL Pipeline:**
- Crawl → Ingest (crawl_data)
- Cleansing & Normalization (tiki_data_3nf)
- Star Schema Transform (tiki_warehouse)

✅ **Data Quality:**
- Loại bỏ NULL brand / NULL seller
- Validate price ranges
- Check data consistency

✅ **Warehouse Schema:**
- Fact table: fact_product_sales (885 records)
- 6 Dimension tables với foreign keys

### 5.2 Phạm Vi Loại Trừ ❌
- **Historical data:** Chỉ snapshot hiện tại (không tracking time series)
- **Customer data:** Không có buyer info, purchase history
- **Competitor data:** Chỉ Tiki, không compare với Shopee/Lazada
- **Supply chain details:** Không warehouse location, logistics cost
- **Real-time data:** Batch process, không live streaming
- **Prediction models:** Chỉ descriptive analytics, không predictive/prescriptive

---

## 6. DANH SÁCH DỮ LIỆU THU THẬP & MÔ TẢ

### 6.1 Bảng FACT: fact_product_sales (885 records)
| Cột | Kiểu | Mô Tả |
|-----|------|-------|
| fact_id | INT (PK) | Unique identifier cho mỗi fact record |
| product_sk | INT (FK) | Foreign key tới dim_product |
| category_sk | INT (FK) | Foreign key tới dim_category |
| seller_sk | INT (FK) | Foreign key tới dim_seller |
| brand_sk | INT (FK) | Foreign key tới dim_brand |
| date_sk | INT (FK) | Foreign key tới dim_date |
| price_segment_sk | INT (FK) | Foreign key tới dim_price_segment |
| **price** | NUMERIC(12,2) | Giá hiện tại (VNĐ) |
| **original_price** | NUMERIC(12,2) | Giá gốc trước discount |
| **discount_percent** | NUMERIC(5,2) | % giảm giá |
| **quantity_sold** | INT | Số lượng bán (crawled sales_count) |
| **estimated_revenue** | NUMERIC(15,2) | Doanh thu ước tính (price × qty) |
| **estimated_profit** | NUMERIC(15,2) | Lợi nhuận ước tính (revenue × 15%) |
| **average_rating** | NUMERIC(3,1) | Đánh giá trung bình (0-5) |
| **rating_count** | INT | Số lượng đánh giá |
| **review_count** | INT | Số lượng reviews |

**Thống kê dữ liệu:**
- Có giá: 885/885 (100%)
- Có rating: 865/885 (97.7%)
- Giá trung bình: ₫827,208.84
- Rating trung bình: 4.35/5

### 6.2 Bảng DIMENSION: dim_product (885 records)
| Cột | Mô Tả |
|-----|-------|
| product_sk | Surrogate key (SERIAL) |
| product_id | Product ID từ Tiki (UNIQUE) |
| product_name | Tên sản phẩm |
| brand | Thương hiệu |
| url | URL sản phẩm |

### 6.3 Bảng DIMENSION: dim_category (209 records)
| Cột | Mô Tả |
|-----|-------|
| category_sk | Surrogate key |
| category_id | Category ID (hashed MD5 từ path) |
| level_1 đến level_5 | Phân cấp danh mục (max 5 levels) |
| full_path | Đường dẫn danh mục đầy đủ |

**Top categories:**
- Dụng cụ nhà bếp > Dụng cụ làm bánh (25 products)
- Nội thất > Nội thất phòng khách (23 products)
- Dụng cụ nhà bếp > Dao, kéo (21 products)

### 6.4 Bảng DIMENSION: dim_seller (252 records)
| Cột | Mô Tả |
|-----|-------|
| seller_sk | Surrogate key |
| seller_id | Seller ID (hashed MD5) |
| seller_name | Tên người bán |

### 6.5 Bảng DIMENSION: dim_brand (267 records)
| Cột | Mô Tả |
|-----|-------|
| brand_sk | Surrogate key |
| brand_name | Tên thương hiệu (UNIQUE) |

**Top brands:**
- OEM: 123 products | Rating: 4.07
- 3M: 35 products | Rating: 4.31
- Diệu Tâm: 27 products | Rating: 4.33

### 6.6 Bảng DIMENSION: dim_date (1 record)
| Cột | Mô Tả |
|-----|-------|
| date_sk | Surrogate key |
| date_value | Date (UNIQUE) |
| year, month, day, quarter, week | Thành phần ngày |

**Hiện tại:** 1 ngày duy nhất (2025-11-26)

### 6.7 Bảng DIMENSION: dim_price_segment (6 records)
| Cột | Mô Tả |
|-----|-------|
| price_segment_sk | Surrogate key |
| segment_name | Tên phân khúc |
| min_price, max_price | Range giá |

**Phân khúc giá:**
| Segment | Số SP | Giá TB | Doanh Thu |
|---------|-------|---------|-----------|
| Rẻ (< 100K) | 399 | ₫48K | ₫19.3B |
| Bình dân (100K-500K) | 279 | ₫220K | ₫61.4B |
| Trung bình (500K-1M) | 61 | ₫700K | ₫42.7B |
| Cao (1M-5M) | 114 | ₫2.2M | ₫253.7B |
| Cao cấp (> 5M) | 32 | ₫11.1M | ₫355.1B |

---

## 7. LUỒNG XỬ LÝ DỮ LIỆU (E2E PIPELINE)

### 7.1 Kiến Trúc Tổng Quan
```
┌─────────────────────────────────────────────────────────────────────┐
│                       TIKI E-COMMERCE DATA PIPELINE                 │
└─────────────────────────────────────────────────────────────────────┘

[LAYER 1: EXTRACT] → [LAYER 2: INGEST] → [LAYER 3: TRANSFORM] → [LAYER 4: LOAD]
                                                                      ↓
                                                        [LAYER 5: VISUALIZE]
```

### 7.2 Chi Tiết Từng Giai Đoạn

#### **GIAI ĐOẠN 1: CRAWL (Extract)**
**Mục đích:** Thu thập dữ liệu thô từ Tiki.vn  
**Công nghệ:** Selenium + Requests + BeautifulSoup

**Quy trình:**
1. Crawl danh sách danh mục (category tree)
2. Crawl danh sách sản phẩm theo từng danh mục
3. Crawl chi tiết sản phẩm (price, rating, seller info)
4. Lưu JSON thô vào files trong `data/raw/`

**Output:** 
- `crawl_data.products` database table
- 1,122 sản phẩm thô (chứa NULL values)

**Thách thức:**
- Rate limiting & blocking từ Tiki
- Inconsistent HTML structure
- Dynamic content phải load JS

---

#### **GIAI ĐOẠN 2: INGEST (Cleansing)**
**Mục đích:** Làm sạch, validate dữ liệu thô  
**Công nghệ:** Python + psycopg2

**Quy trình:**
1. Loại bỏ records với NULL/empty trường quan trọng
   - `brand IS NULL` → 5 products removed
   - `seller_name IS NULL` → 232 products removed
   - **Kết quả:** 885 sản phẩm hợp lệ (78.9%)

2. Normalize dữ liệu:
   - Chuẩn hóa giá (chuyển thành numeric)
   - Tính toán discount%
   - Extract category path (JSON → array)
   - Parse seller info

3. Compute derived fields:
   - `estimated_revenue = price × quantity`
   - `estimated_profit = revenue × 15%` (default margin)
   - `profit_margin_percent`

4. Data quality checks:
   - Price > 0
   - Rating trong [0, 5]
   - Kiểm tra consistency

**Output:** 
- `crawl_data.products` (885 valid records sau delete)
- Ready for next phase

---

#### **GIAI ĐOẠN 3: TRANSFORM (Normalization)**
**Mục đích:** Chuẩn hóa dữ liệu theo 3NF (Third Normal Form)  
**Công nghệ:** Python + psycopg2

**Quy trình:**
1. Tạo database `tiki_data_3nf`
2. Tạo 5 bảng normalized:
   - `products` (main fact table 1:M with others)
   - `categories` (category hierarchy)
   - `sellers` (unique sellers)
   - `ratings` (rating aggregates)
   - `product_categories` (many-to-many)

3. Extract & normalize từng entity:
   - Tách category path → tạo dim_category
   - Tách seller info → tạo dim_seller
   - Assign surrogate keys (MD5 hashes)

**Database schema (3NF):**
```sql
-- Gán PK, FK, unique constraints
-- Normalization form: 3NF (no partial/transitive dependencies)
-- Index on frequently queried columns
```

**Output:**
- `tiki_data_3nf` database với 5 tables
- 885 products, 209 categories, 252 sellers
- Fully normalized, no data duplication

---

#### **GIAI ĐOẠN 4: LOAD (Star Schema Transformation)**
**Mục đích:** Chuyển từ 3NF sang Star Schema (OLAP-optimized)  
**Công nghệ:** Python + psycopg2

**Quy trình:**
1. Tạo database `tiki_warehouse`
2. Tạo Star Schema:
   - **1 FACT table:** fact_product_sales (885 rows)
   - **6 DIM tables:** product, category, seller, brand, date, price_segment

3. Load dữ liệu:
   - Extract từ crawl_data (raw products)
   - Filter: loại bỏ NULL brand/seller lần nữa (safety check)
   - Map thành surrogate keys
   - Insert vào warehouse

4. Tạo indexes:
   - Clustered index trên PK
   - Non-clustered indexes trên FK columns
   - Index trên frequently filtered columns

**Star Schema Benefits:**
```
┌──────────────────┐
│   DIM_PRODUCT    │
│  (885 products)  │
└────────┬─────────┘
         │
         ├─→ FK: product_sk
         │
    ┌────┴────────────────────────┐
    │                             │
┌───┴────────┐    ┌──────────────┴───┐
│ DIM_SELLER │    │  FACT_PRODUCT_   │
│  (252)     │────│    SALES (885)   │
└────────────┘    └──────────────┬───┘
                                 │
    ┌────────────────────────────┼────────────────────┐
    │                            │                    │
┌───┴────────┐   ┌──────────────┴───┐   ┌──────────┴──┐
│DIM_CATEGORY│   │  DIM_BRAND       │   │DIM_PRICE_   │
│  (209)     │   │  (267)           │   │SEGMENT (6)  │
└────────────┘   └──────────────────┘   └─────────────┘

Star Schema = Denormalization for OLAP (high read performance)
```

**Output:**
- `tiki_warehouse` database với 7 tables
- 885 fact records, 6 dimensions
- Ready for BI tools & analytics queries

---

#### **GIAI ĐOẠN 5: VISUALIZE & ANALYZE**
**Mục đích:** Truy vấn, visualize, tạo insights  
**Công nghệ:** SQL + Power BI / Tableau / Python Visualization

**Ví dụ Queries:**
```sql
-- Q1: Top 10 brands by product count
SELECT brand_name, COUNT(*) FROM fact_product_sales
JOIN dim_brand ON ... GROUP BY brand_name ORDER BY COUNT(*) DESC;

-- Q2: Average rating by category
SELECT full_path, AVG(average_rating) FROM fact_product_sales
JOIN dim_category ON ... GROUP BY full_path;

-- Q3: Price vs Rating correlation
SELECT price_segment, AVG(average_rating), COUNT(*) FROM fact_product_sales
JOIN dim_price_segment ON ... GROUP BY price_segment;

-- Q4: Revenue by brand
SELECT brand_name, SUM(estimated_revenue) FROM fact_product_sales
JOIN dim_brand ON ... GROUP BY brand_name ORDER BY SUM DESC;
```

**Potential Dashboards:**
1. **Executive Overview:** KPIs (total products, avg rating, total revenue)
2. **Category Performance:** Sales by category, trending categories
3. **Brand Analysis:** Top brands, brand quality scorecard
4. **Pricing Strategy:** Price distribution, discount impact
5. **Seller Quality:** Seller metrics, reliability index

---

### 7.3 Data Flow Diagram
```
TIKI WEBSITE
    │
    ├─→ Crawl Categories → JSON
    ├─→ Crawl Products List → JSON
    └─→ Crawl Product Details → JSON
         │
         ↓
┌─────────────────────────┐
│  crawl_data.products    │ ← 1,122 raw products
│  (Raw, may have NULLs)  │
└────────────┬────────────┘
             │ [FILTER: Remove NULL brand/seller]
             ↓
┌─────────────────────────┐
│  tiki_data_3nf          │ ← 885 clean products
│  (3NF normalized)       │
│  - products             │
│  - categories           │
│  - sellers              │
│  - ratings              │
│  - product_categories   │
└────────────┬────────────┘
             │ [TRANSFORM: Extract dims, create FK]
             ↓
┌─────────────────────────┐
│  tiki_warehouse         │ ← STAR SCHEMA
│  (OLAP optimized)       │
│  - fact_product_sales   │
│  - dim_product          │
│  - dim_category         │
│  - dim_seller           │
│  - dim_brand            │
│  - dim_date             │
│  - dim_price_segment    │
└────────────┬────────────┘
             │
    ┌────────┴─────────┐
    │                  │
    ↓                  ↓
[SQL Queries]    [BI Tools]
    │                  │
    ├─→ Reports    ├─→ Power BI Dashboards
    ├─→ Analytics  ├─→ Tableau Visualizations
    └─→ Insights   └─→ Executive Reports
```

---

## 8. CÔNG NGHỆ SỬ DỤNG & LÝ DO LỰA CHỌN

### 8.1 Stack Công Nghệ

| Thành Phần | Công Nghệ | Phiên Bản | Lý Do Chọn |
|-----------|-----------|---------|-----------|
| **Crawling** | Selenium + BeautifulSoup | Latest | Web scraping động (handle JS), robust |
| **Data Processing** | Python 3.14 | 3.14 | Flexible, strong ecosystem (pandas, numpy) |
| **Database (OLTP)** | PostgreSQL | 14+ | Reliable, mở rộng, open-source, ACID compliance |
| **Database (OLAP)** | PostgreSQL (Star Schema) | 14+ | Same stack, tối ưu với thích hợp indexes |
| **Orchestration** | Apache Airflow | 2.5+ | DAG-based, scheduling, monitoring, scaling |
| **BI/Visualization** | Power BI hoặc Tableau | Latest | Industry standard, rich visualizations |
| **Deployment** | Docker Compose | Latest | Containerization, reproducible environment |

### 8.2 Lý Do Lựa Chọn Kiến Trúc

#### **Tại sao Star Schema?**
✅ **Pros:**
- **Performance:** Fact table queries nhanh hơn 10-100x so với 3NF (do ít JOINs)
- **Simplicity:** Dễ hiểu cho BI developers, không cần complex JOIN logic
- **Flexibility:** Dễ mở rộng thêm measures (facts) hoặc dimensions mới
- **Standard:** Industry-standard cho Data Warehousing (widely adopted)

❌ **Cons:**
- **Storage:** Denormalization → tăng dung lượng (có thể tối ưu với compression)
- **Update complexity:** Nếu dimension thay đổi, cần maintain history (slowly changing dimensions)
- **Data redundancy:** Dữ liệu có thể bị duplicate giữa fact & dims

#### **Tại sao PostgreSQL?**
✅ **Pros:**
- **Open-source:** Free, không license cost
- **Reliable:** ACID compliance, transaction support, replication
- **Powerful:** JSON support (JSONB), window functions, CTEs
- **Ecosystem:** Airflow có native support, widely used
- **Scalability:** Partitioning, sharding options để mở rộng

❌ **Cons:**
- **Not columnar:** Dữ liệu lưu theo row (không optimal cho OLAP)
  → *Solution:* Có thể upgrade lên Citus, TimescaleDB, hoặc chuyển sang Snowflake/BigQuery sau

#### **Tại sao Airflow?**
✅ **Pros:**
- **DAG-based:** Flexible, dễ định nghĩa dependencies
- **Monitoring:** UI dashboards, alerting, retry logic
- **Scalability:** Distributed execution (Celery, Kubernetes)
- **Community:** Lớn, open-source, nhiều extensions

❌ **Cons:**
- **Complexity:** Setup & configuration phức tạp (learning curve)
  → *Solution:* Hiện tại dùng local Docker, có thể scale sau

---

### 8.3 Lựa Chọn Khác & Trade-offs

| Alternative | Pros | Cons | Decision |
|------------|------|------|----------|
| **BigQuery (Google Cloud)** | Fully managed, columnar, fast | Costly, vendor lock-in | Future option khi scale |
| **Snowflake** | Cloud native, elastic scaling | High cost | Premium option |
| **Spark** | Distributed processing | Overkill cho quy mô hiện tại | Use later if > 10M records |
| **MongoDB** | Schema flexibility | Poor for analytics | Not suitable |

---

## 9. KỲ VỌNG KẾT QUẢ (DELIVERABLES)

### 9.1 Deliverables Đã Hoàn Thành ✅

#### **Phase 1: Data Infrastructure**
- ✅ ETL Pipeline hoàn chỉnh: Crawl → Ingest → Transform → Load
- ✅ PostgreSQL Database với 3 layers:
  - `crawl_data` (1,122 raw products)
  - `tiki_data_3nf` (885 cleaned, 3NF normalized)
  - `tiki_warehouse` (885 products, Star Schema OLAP)
- ✅ Data Quality Gates: loại bỏ NULL brand/seller (237 products removed)

#### **Phase 2: Star Schema Warehouse**
Hoàn thành 7 bảng:
- ✅ **fact_product_sales** (885 records) với 15 columns (price, rating, revenue, profit, etc.)
- ✅ **dim_product** (885 unique products)
- ✅ **dim_category** (209 categories, hierarchical 5-levels)
- ✅ **dim_seller** (252 sellers)
- ✅ **dim_brand** (267 brands)
- ✅ **dim_date** (1 date, expandable)
- ✅ **dim_price_segment** (6 price tiers)

#### **Phase 3: Database Optimization**
- ✅ Surrogate keys (SERIAL) cho tất cả dimensions
- ✅ Foreign Key constraints (referential integrity)
- ✅ Indexes trên FK & frequently queried columns
- ✅ Data types optimized (NUMERIC(12,2) cho giá, NUMERIC(3,1) cho rating)

#### **Phase 4: Data Quality Metrics**
Dữ liệu warehouse:
- 100% có giá (885/885)
- 97.7% có rating (865/885)
- Giá trung bình: ₫827K
- Rating trung bình: 4.35/5 (khá tốt)

---

### 9.2 Deliverables Sắp Phát Triển (Next Phase) 🚀

#### **Phase 5: BI & Visualization (Roadmap)**
Kế hoạch:
- [ ] **Power BI Dashboard** (3-4 pages):
  - Executive Summary (KPIs, trends)
  - Category Performance (top/bottom categories)
  - Brand Analysis (market share, quality)
  - Pricing & Discount Impact Analysis
  
- [ ] **SQL Saved Queries** (10+ canned reports):
  - Top products by rating
  - Revenue by category
  - Seller quality scorecard
  - Price elasticity analysis
  
- [ ] **Automated Reports**:
  - Daily/Weekly snapshots
  - Email distribution
  - PDF exports

#### **Phase 6: Advanced Analytics**
- [ ] **Time Series Analysis**:
  - Track metrics over time (need to expand dim_date)
  - Trend detection, seasonality
  
- [ ] **Cohort Analysis**:
  - Product cohorts (price, category, brand)
  - Perform year-over-year comparison (when data > 1 year)
  
- [ ] **Predictive Models** (future):
  - Price optimization
  - Demand forecasting
  - Product recommendation engine

---

### 9.3 Kỳ Vọng Output cho Mỗi Stakeholder

#### **For Business Analysts**
- Dữ liệu sạch, sẵn sàng query
- Star Schema đơn giản (5 JOINs tối đa)
- Performance: < 1 sec cho bất kỳ aggregate query
- **KPI Dashboard** để track business metrics

#### **For Marketing Team**
- **Category Insights:** Top categories, emerging trends
- **Brand Performance:** Market share, customer sentiment (via rating)
- **Competitor Analysis:** Within Tiki universe (seller performance)
- **Campaign Insights:** Price elasticity, discount effectiveness

#### **For Operations**
- **Inventory Optimization:** Products by velocity (sales_count)
- **Seller Quality:** Reliable sellers, quality scores
- **Pricing Strategy:** Price points by category, margin analysis

#### **For Data Scientists** (future)
- Clean data as input to ML models
- Features: price, rating, category, brand, seller
- Target: predict sales, recommend products

---

## 10. KHÁC KHĂN & HƯỚNG PHÁT TRIỂN THÊM

### 10.1 Thách Thức Gặp Phải

#### **1. Dữ Liệu Thiếu Toàn Vẹn** ⚠️
- **Vấn đề:** 237/1122 products bị loại bỏ do NULL brand/seller (21.1%)
  - Nguyên nhân: Tiki không mandatory require sellers phải fill brand field
  - Impact: Data completeness chỉ 78.9%

- **Giải pháp:**
  - Crawl thêm từ các sources khác (official APIs, competitor data)
  - Implement data imputation (fill NULL với "Unknown", "Generic")
  - Manual review for high-value products

#### **2. Dữ Liệu Snapshot, Không Time Series** ⏰
- **Vấn đề:** Hiện tại chỉ có 1 ngày dữ liệu (2025-11-26)
  - Không thể track trends, seasonality
  - Rating/price có thể thay đổi → need versioning

- **Giải pháp:**
  - Implement incremental crawl (daily/weekly)
  - Expand dim_date với proper time keys
  - Implement Slowly Changing Dimensions (SCD Type 2):
    ```sql
    ALTER TABLE dim_product ADD COLUMN effective_date, end_date, is_current
    ```
  - Store historical data (dimension tables + fact snapshots)

#### **3. Dữ liệu Accuracy** 📊
- **Vấn đề:**
  - sales_count là crawler estimate, không actual data
  - estimated_profit dùng default 15% margin (sai với reality)
  - Rating có thể outdated (crawled once, no refresh)

- **Giải pháp:**
  - Validate against Tiki official metrics (if API available)
  - Survey sellers để lấy actual margin data
  - Refresh data regularly (daily crawl)

#### **4. Scalability** 📈
- **Vấn đề:** 885 products đủ cho MVP, nhưng:
  - Tiki có 1M+ products
  - Crawling time: linear (mỗi product ~ 1-2 sec)
  - Storage: PostgreSQL row-based không optimal cho 1B+ records

- **Giải pháp:**
  - Implement distributed crawling (Scrapy, multiprocessing)
  - Use columnar database (Snowflake, BigQuery) khi scale
  - Implement partitioning by category/brand
  - Use caching (Redis) để avoid re-crawling

#### **5. Data Governance & Quality** 🛡️
- **Vấn đề:**
  - Chưa có formal data quality SLA
  - Chưa có metadata documentation (data dictionary)
  - Chưa có access control (ai có quyền access what)

- **Giải pháp:**
  - Implement data quality framework (Great Expectations)
  - Create comprehensive data dictionary
  - Implement role-based access control (RBAC)
  - Setup data lineage tracking (dbt, Lineage tools)

---

### 10.2 Hướng Phát Triển Thêm 🚀

#### **Short-term (1-2 months)**
1. **Add Historical Tracking**
   - Daily incremental crawl
   - Implement SCD Type 2 in warehouse
   - Track price changes, rating trends

2. **Enhance BI Dashboards**
   - Build 5+ interactive Power BI dashboards
   - Real-time metrics via DirectQuery
   - Drill-down capabilities (Dashboard → Detail Report)

3. **Data Quality Automation**
   - Great Expectations framework
   - Automated quality checks & alerts
   - Data profiling (nullness, uniqueness, range checks)

4. **API Integration**
   - If Tiki opens API: sync official data
   - Validate crawled data against API
   - Reduce crawling frequency (API > Crawling)

#### **Mid-term (2-6 months)**
1. **Expand Data Coverage**
   - Add customer reviews text (sentiment analysis)
   - Add competitor data (Shopee, Lazada)
   - Add supply chain data (warehouse, logistics cost)

2. **Advanced Analytics**
   - Product clustering (similar products)
   - Price optimization models
   - Demand forecasting (ARIMA, Prophet, LSTM)

3. **ML Pipeline**
   - Product recommendation engine
   - Churn prediction for sellers
   - Price elasticity modeling

4. **Cloud Migration**
   - Move to AWS/GCP (RDS/Cloud SQL → Redshift/BigQuery)
   - Implement cloud ETL (AWS Glue, GCP Dataflow)
   - Cost optimization & autoscaling

#### **Long-term (6-12 months)**
1. **Real-time Analytics**
   - Stream processing (Kafka + Spark Streaming)
   - Real-time dashboards
   - Alerting on anomalies

2. **Advanced Monetization**
   - Data product (sell insights to sellers)
   - Premium analytics (subscription model)
   - API untuk 3rd-party consumers

3. **Global Expansion**
   - Multi-region data warehouse
   - Cross-border product analysis
   - Multi-currency support

---

## 11. KẾT LUẬN

### 11.1 Tóm Tắt Thành Quả
Dự án **Tiki Data Warehouse** đã thành công hoàn thành giai đoạn ETL & Star Schema:

✅ **Data Infrastructure:**
- 3-layer data pipeline: Raw → Clean → Analytics
- 885 high-quality sản phẩm sau filtering
- Star Schema warehouse tối ưu cho OLAP queries

✅ **Data Quality:**
- 100% có giá, 97.7% có rating
- NULL handling: loại bỏ 237 invalid products
- Average rating 4.35/5 (customer satisfaction good)

✅ **Technological Foundation:**
- PostgreSQL (RDBMS)
- Python (data processing)
- Airflow (orchestration, future)
- Star Schema (analytics-ready)

✅ **Business Readiness:**
- Warehouse sẵn sàng cho BI tools (Power BI, Tableau)
- 10+ canned queries có thể support ngay
- Scalable architecture cho 10x growth

### 11.2 Giá Trị Mang Lại
1. **Data-Driven Decisions:** Business team có dữ liệu chính xác để quyết định
2. **Market Intelligence:** Hiểu rõ market trends, competitor moves (within Tiki)
3. **Operational Efficiency:** Optimize pricing, inventory, seller quality
4. **Scalable Foundation:** Ready để mở rộng to millions of products

### 11.3 Tiếp Theo
**Immediate priorities:**
1. Deploy Power BI dashboards (1-2 tuần)
2. Setup daily crawl (2 tuần)
3. Implement quality monitoring (1 tuần)

**Critical success factors:**
- Regular data refresh (daily/weekly)
- Stakeholder feedback loops
- Continuous quality monitoring

---

## 12. THAM KHẢO & TÀI NGUYÊN

### 12.1 Công Nghệ
- PostgreSQL Documentation: https://www.postgresql.org/docs/
- Apache Airflow: https://airflow.apache.org/
- Star Schema Design: Kimball's "The Data Warehouse Toolkit"

### 12.2 Chuẩn Mực
- Data Warehouse Best Practices: Kimball, Inmon methodologies
- SQL Style Guide: https://www.sqlstyle.guide/
- Data Quality: DAMA Framework, Gartner

### 12.3 Liên Hệ & Support
- Data Engineer: [Contact Info]
- Analytics Lead: [Contact Info]
- Documentation: [Github/Wiki URL]

---

**Document Version:** 1.0  
**Last Updated:** 2025-11-26  
**Next Review:** 2025-12-26  
**Status:** ✅ Active

---

*Tài liệu này dành cho nhóm Data, Analytics, Business Intelligence. Phiên bản cập nhật sẽ được phân phối khi có thay đổi lớn.*
