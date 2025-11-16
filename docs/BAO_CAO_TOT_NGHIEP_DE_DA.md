# BÁO CÁO TỐT NGHIỆP
## HỆ THỐNG DATA PIPELINE VÀ DATA ANALYTICS
### Thu thập, Xử lý và Phân tích Dữ liệu Sản phẩm Tiki.vn

---

**Tác giả:** [Tên Sinh viên]  
**Ngành:** Khoa học Dữ liệu / Kỹ thuật Phần mềm  
**Năm:** 2024-2025

---

## MỤC LỤC

1. [Giới thiệu Dự án](#1-giới-thiệu-dự-án)
2. [Kiến trúc Tổng thể (Data Platform Architecture)](#2-kiến-trúc-tổng-thể-data-platform-architecture)
3. [Luồng Dữ liệu End-to-End (DE + DA Workflow)](#3-luồng-dữ-liệu-end-to-end-de--da-workflow)
4. [Quy trình ETL / ELT Chi tiết](#4-quy-trình-etl--elt-chi-tiết)
5. [Data Model (ERD + Star Schema + Data Mart)](#5-data-model-erd--star-schema--data-mart)
6. [Business Logic](#6-business-logic)
7. [Phân tích Dữ liệu (DA Section)](#7-phân-tích-dữ-liệu-da-section)
8. [Dashboard Design](#8-dashboard-design)
9. [Scheduling & Orchestration](#9-scheduling--orchestration)
10. [Monitoring – Data Quality – Error Handling](#10-monitoring--data-quality--error-handling)
11. [Sơ đồ Tổng hợp](#11-sơ-đồ-tổng-hợp)

---

# 1. GIỚI THIỆU DỰ ÁN

## 1.1. Mục tiêu Tổng thể của Dự án DE & DA

Dự án **Tiki Data Pipeline** được xây dựng nhằm mục tiêu tạo ra một hệ thống tự động hóa hoàn chỉnh để thu thập, xử lý, lưu trữ và phân tích dữ liệu sản phẩm từ nền tảng thương mại điện tử Tiki.vn. Hệ thống kết hợp các nguyên tắc và thực hành tốt nhất của **Data Engineering (DE)** và **Data Analytics (DA)** để cung cấp dữ liệu sạch, chuẩn hóa và có giá trị cho các hoạt động phân tích kinh doanh.

### Mục tiêu cụ thể:

**Về Data Engineering:**
- Xây dựng pipeline tự động hóa (ETL/ELT) để thu thập dữ liệu từ nguồn web động (Tiki.vn)
- Chuẩn hóa và làm sạch dữ liệu thô thành dữ liệu có cấu trúc, sẵn sàng cho phân tích
- Lưu trữ dữ liệu theo mô hình Data Warehouse với khả năng mở rộng và truy vấn hiệu quả
- Đảm bảo chất lượng dữ liệu, độ tin cậy và khả năng phục hồi của hệ thống

**Về Data Analytics:**
- Cung cấp cơ sở dữ liệu sẵn sàng cho các phân tích kinh doanh (BI)
- Tính toán các chỉ số KPI và metrics quan trọng (doanh thu, độ phổ biến, giá trị sản phẩm)
- Hỗ trợ các câu hỏi phân tích như: xu hướng thị trường, sản phẩm bán chạy, phân tích giá cả
- Tạo điều kiện cho việc xây dựng dashboard và báo cáo trực quan

## 1.2. Bài toán Thực tế Cần Giải quyết

### 1.2.1. Thách thức từ Thương mại Điện tử

Trong bối cảnh thương mại điện tử phát triển mạnh mẽ tại Việt Nam, việc theo dõi và phân tích dữ liệu sản phẩm trở nên quan trọng cho nhiều bên:

- **Đối với Nhà bán hàng:** Cần theo dõi giá cả, xu hướng bán hàng, đối thủ cạnh tranh
- **Đối với Nhà phân tích Thị trường:** Cần dữ liệu để nghiên cứu xu hướng, phân đoạn thị trường
- **Đối với Nhà đầu tư:** Cần hiểu hiệu suất của nền tảng, sản phẩm bán chạy
- **Đối với Người tiêu dùng:** Cần so sánh giá cả, đánh giá chất lượng sản phẩm

### 1.2.2. Vấn đề Kỹ thuật

- **Nguồn dữ liệu động:** Tiki.vn là website động, dữ liệu được render bởi JavaScript, không thể crawl đơn giản bằng HTTP requests
- **Quy mô dữ liệu lớn:** Hàng triệu sản phẩm, hàng nghìn danh mục, dữ liệu thay đổi liên tục
- **Cần xử lý theo thời gian thực:** Dữ liệu cần được cập nhật thường xuyên để đảm bảo tính chính xác
- **Đảm bảo chất lượng:** Dữ liệu cần được validate, làm sạch và chuẩn hóa trước khi phân tích

## 1.3. Tại sao Cần Xây dựng Data Pipeline + Dashboard

### 1.3.1. Tự động hóa Quy trình Thu thập

Thay vì thu thập dữ liệu thủ công (tốn thời gian, dễ sai sót), pipeline tự động hóa toàn bộ quy trình:
- Crawl dữ liệu theo lịch định kỳ (daily/hourly)
- Xử lý lỗi tự động và retry khi cần
- Theo dõi trạng thái và thông báo khi có vấn đề

### 1.3.2. Chuẩn hóa và Làm sạch Dữ liệu

Pipeline đảm bảo dữ liệu được:
- Chuẩn hóa format (số, ngày tháng, text)
- Validate theo quy tắc nghiệp vụ
- Tính toán các trường derived (revenue, score, category)

### 1.3.3. Tích hợp với Hệ thống Phân tích

Dữ liệu sau pipeline sẵn sàng cho:
- Query SQL để phân tích ad-hoc
- Kết nối với BI tools (Power BI, Tableau, Superset)
- Xây dựng dashboard trực quan
- Phân tích machine learning

## 1.4. Nguồn Dữ liệu

### 1.4.1. Nguồn Chính: Tiki.vn Website

**Loại dữ liệu thu thập:**
- **Danh mục sản phẩm (Categories):**
  - Tên danh mục, slug, URL
  - Cấu trúc phân cấp (parent-child)
  - Hình ảnh danh mục
  - Số lượng sản phẩm mỗi danh mục

- **Thông tin Sản phẩm (Products):**
  - ID sản phẩm, tên, URL, hình ảnh
  - Giá hiện tại, giá gốc, phần trăm giảm giá
  - Số lượng đã bán (sales_count)
  - Đánh giá trung bình, số lượng review
  - Thông tin người bán (seller)
  - Thương hiệu (brand)
  - Mô tả sản phẩm, thông số kỹ thuật
  - Tình trạng kho hàng (stock)

- **Dữ liệu Bổ sung:**
  - Shipping options
  - Hình ảnh sản phẩm (multiple)
  - Lịch sử giá (nếu crawl nhiều lần)

### 1.4.2. Phương thức Thu thập

- **HTTP/HTTPS Requests:** Cho dữ liệu tĩnh (HTML parsing với BeautifulSoup)
- **Selenium WebDriver:** Cho dữ liệu động (JavaScript-rendered content)
- **API (nếu có):** Tiki API (nếu được cung cấp công khai)

## 1.5. Phạm vi Sử dụng Dữ liệu

### 1.5.1. Ứng dụng Nghiệp vụ

- **Phân tích Thị trường:** Xu hướng giá cả, sản phẩm bán chạy
- **So sánh Giá cả:** Giá sản phẩm giữa các nhà bán hàng
- **Phân tích Đối thủ:** Hiểu chiến lược giá, danh mục của đối thủ
- **Dự đoán Xu hướng:** Machine learning để dự đoán giá, nhu cầu

### 1.5.2. Ứng dụng Kỹ thuật

- **Data Warehouse:** Lưu trữ dữ liệu lịch sử cho phân tích
- **Data Mart:** Tổng hợp dữ liệu theo chủ đề (products, sales, pricing)
- **BI Dashboard:** Trực quan hóa dữ liệu cho quản lý
- **API Service:** Cung cấp dữ liệu cho các ứng dụng khác

## 1.6. Đối tượng Sử dụng

### 1.6.1. Người dùng Cuối (End Users)

- **Quản lý Kinh doanh:** Xem dashboard tổng quan, xu hướng
- **Nhà Phân tích Kinh doanh (Business Analyst):** Phân tích chi tiết, tạo báo cáo
- **Nhà Quản lý Sản phẩm:** Theo dõi sản phẩm, danh mục

### 1.6.2. Người dùng Kỹ thuật

- **Data Analyst:** Query SQL, phân tích dữ liệu, tạo insights
- **Data Engineer:** Quản lý pipeline, tối ưu hiệu suất
- **Data Scientist:** Xây dựng model machine learning

## 1.7. Phạm vi Dự án

### 1.7.1. Ingestion Layer

- Crawl categories từ Tiki.vn (đệ quy)
- Crawl danh sách products từ mỗi category
- Crawl chi tiết product (price, rating, description, etc.)
- Xử lý rate limiting và error handling

### 1.7.2. ETL/ELT Layer

- **Extract:** Lấy dữ liệu từ raw JSON files
- **Transform:** Normalize, validate, compute derived fields
- **Load:** Lưu vào PostgreSQL database và JSON backup

### 1.7.3. Storage Layer

- **Raw Zone:** Lưu dữ liệu thô (JSON files)
- **Cleansed Zone:** Dữ liệu đã được làm sạch
- **Transformed Zone:** Dữ liệu đã transform và có computed fields
- **Data Warehouse:** PostgreSQL database với schema chuẩn hóa

### 1.7.4. Analytics Layer

- Tính toán KPI và metrics
- Tạo Data Mart cho BI
- Hỗ trợ SQL queries

### 1.7.5. Dashboard Layer

- Thiết kế dashboard với Power BI / Superset / Tableau
- Các trang: Overview, Detail, Drill-down
- Storytelling data

---

# 1.8. Cấu trúc Dự án và Module

## 1.8.1. Cấu trúc Thư mục Tổng thể

```
tiki-data-pipeline/
├── 📄 README.md                    # Tài liệu hướng dẫn chính
├── 📄 LICENSE                      # MIT License
├── 🐳 docker-compose.yaml         # Docker Compose configuration
├── 📄 requirements.txt             # Python dependencies
├── 📄 pyproject.toml               # Python project configuration
├── 📄 Makefile                     # Make commands cho development
│
├── 📚 docs/                        # Documentation
│   ├── BAO_CAO_TOT_NGHIEP_DE_DA.md # Báo cáo tốt nghiệp (file này)
│   ├── ARCHITECTURE.md             # Tài liệu kiến trúc
│   ├── CONNECT_DATABASE_LOCAL.md   # Hướng dẫn kết nối database
│   ├── OPTIMIZATION_GUIDE.md       # Hướng dẫn tối ưu
│   ├── CACHE_CONFIGURATION.md      # Cấu hình cache
│   └── *.md, *.mmd, *.puml, *.xml # Các diagram files
│
├── ☁️ airflow/                     # Airflow configuration
│   ├── dags/                       # Airflow DAGs
│   │   ├── tiki_crawl_products_dag.py      # DAG chính
│   │   ├── tiki_crawl_products_test_dag.py # DAG test
│   │   ├── dag_assets/            # Asset definitions
│   │   ├── dag_helpers/           # Helper functions
│   │   └── dag_tasks/             # Task definitions
│   ├── logs/                       # Airflow logs
│   ├── config/                     # Airflow config
│   │   └── airflow.cfg             # Airflow configuration file
│   ├── plugins/                    # Airflow plugins
│   ├── setup/                      # Setup scripts
│   │   ├── init-airflow-db.sh     # Database init script
│   │   ├── init-crawl-db.sh       # Crawl database init
│   │   └── *.sql, *.py, *.sh      # SQL và setup scripts
│   └── Dockerfile                  # Custom Airflow image với Chrome
│
├── 💻 src/                         # Source code
│   ├── pipelines/                  # Pipeline modules
│   │   ├── crawl/                 # Crawling pipelines
│   │   │   ├── crawl_categories_recursive.py    # Crawl categories đệ quy
│   │   │   ├── crawl_categories_optimized.py    # Crawl categories tối ưu
│   │   │   ├── crawl_products.py                # Crawl danh sách sản phẩm
│   │   │   ├── crawl_products_detail.py         # Crawl chi tiết sản phẩm
│   │   │   ├── extract_category_link_selenium.py # Extract links với Selenium
│   │   │   ├── build_category_tree.py            # Xây dựng category tree
│   │   │   ├── config.py                         # Configuration
│   │   │   ├── utils.py                          # Utility functions
│   │   │   ├── error_handling.py                 # Error handling
│   │   │   ├── resilience/                       # Resilience patterns
│   │   │   │   ├── exceptions.py                 # Custom exceptions
│   │   │   │   ├── circuit_breaker.py            # Circuit breaker pattern
│   │   │   │   ├── dead_letter_queue.py          # Dead letter queue
│   │   │   │   ├── graceful_degradation.py       # Graceful degradation
│   │   │   │   └── error_handler.py              # Error handler tích hợp
│   │   │   ├── storage/                           # Storage modules
│   │   │   │   ├── redis_cache.py                # Redis cache
│   │   │   │   ├── multi_level_cache.py          # Multi-level cache
│   │   │   │   ├── postgres_storage.py           # PostgreSQL storage
│   │   │   │   └── compression.py                # Data compression
│   │   │   └── utils/                            # Utility modules
│   │   │       └── batch_processor.py             # Batch processing
│   │   ├── transform/             # Transform pipeline
│   │   │   └── transformer.py                    # Data transformer
│   │   ├── load/                  # Load pipeline
│   │   │   └── loader.py                         # Data loader
│   │   └── extract/               # Extract utilities
│   │       └── extract_categories.py             # Extract categories
│   └── common/                     # Common modules
│       ├── config.py              # Common configuration
│       ├── ai/                     # AI utilities
│       │   └── summarizer.py      # AI summarization
│       ├── analytics/              # Analytics utilities
│       │   └── aggregator.py      # Data aggregation
│       └── notifications/          # Notification modules
│           └── discord.py          # Discord notifications
│
├── 📊 data/                        # Dữ liệu
│   ├── raw/                        # Raw data (từ crawl)
│   │   ├── categories_recursive_optimized.json
│   │   ├── categories_tree.json
│   │   ├── categories.json
│   │   └── products/
│   │       ├── products.json
│   │       ├── products_with_detail.json
│   │       └── cache/              # Cache files
│   ├── processed/                  # Processed data (sau transform)
│   │   ├── products_transformed.json
│   │   └── products_final.json
│   ├── demo/                       # Demo data
│   └── test_output/                 # Test output
│
├── 📚 demos/                       # Demo files
│   ├── demo_step1_crawl.py        # Demo crawl
│   ├── demo_step2_transform.py     # Demo transform
│   ├── demo_step3_load.py         # Demo load
│   └── demo_e2e_full.py           # Demo full pipeline
│
├── 🔧 scripts/                     # Utility scripts
│   ├── setup/                      # Setup scripts
│   ├── utils/                      # Utility scripts
│   ├── helper/                     # Helper scripts
│   └── *.sh, *.ps1, *.py          # Shell và Python scripts
│
└── 🧪 tests/                       # Test files
    ├── test_crawl_products.py
    ├── test_crawl_recursive.py
    ├── test_transform_load.py
    ├── check_code_quality.py
    ├── setup_airflow_variables.py
    └── pg/                         # PostgreSQL tests
```

## 1.8.2. Module Chính và Nhiệm vụ

### 1.8.2.1. Module `src/pipelines/crawl/`

**Nhiệm vụ:** Thu thập dữ liệu từ Tiki.vn website

**Các file chính:**

**`crawl_categories_recursive.py`**
- **Nhiệm vụ:** Crawl danh mục sản phẩm đệ quy từ Tiki.vn
- **Input:** Tiki.vn website
- **Output:** `data/raw/categories_recursive_optimized.json`
- **Chức năng:** 
  - Parse HTML để lấy danh mục
  - Đệ quy crawl sub-categories
  - Lưu cấu trúc phân cấp (parent-child)

**`crawl_products.py`**
- **Nhiệm vụ:** Crawl danh sách sản phẩm từ mỗi category
- **Input:** Category URLs
- **Output:** `data/raw/products/products.json`
- **Chức năng:**
  - Crawl từng trang sản phẩm (pagination)
  - Extract: product_id, name, url, image_url, sales_count
  - Xử lý rate limiting và error handling

**`crawl_products_detail.py`**
- **Nhiệm vụ:** Crawl chi tiết sản phẩm (giá, rating, mô tả, v.v.)
- **Input:** Product URLs
- **Output:** `data/raw/products/products_with_detail.json`
- **Chức năng:**
  - Sử dụng Selenium để crawl dynamic content
  - Extract: price, rating, description, specifications, images, seller, brand
  - Cache với Redis để tránh crawl lại

**`config.py`**
- **Nhiệm vụ:** Quản lý cấu hình cho crawl pipeline
- **Chức năng:**
  - Đọc environment variables
  - Cấu hình timeout, delay, retry
  - Cấu hình database và cache connections

**`utils.py`**
- **Nhiệm vụ:** Utility functions cho crawling
- **Chức năng:**
  - Parse HTML với BeautifulSoup
  - Xử lý requests với retry
  - Normalize URLs và text
  - Helper functions cho logging

**`error_handling.py`**
- **Nhiệm vụ:** Xử lý lỗi cơ bản
- **Chức năng:**
  - Custom exception classes
  - Error logging
  - Retry logic

### 1.8.2.2. Module `src/pipelines/crawl/resilience/`

**Nhiệm vụ:** Các pattern để tăng độ tin cậy của pipeline

**Các file:**

**`exceptions.py`**
- **Nhiệm vụ:** Định nghĩa custom exceptions
- **Chức năng:**
  - `CrawlError`: Base exception cho crawl errors
  - `NetworkError`, `ParseError`, `ValidationError`: Specific errors
  - `classify_error()`: Phân loại error types

**`circuit_breaker.py`**
- **Nhiệm vụ:** Circuit breaker pattern
- **Chức năng:**
  - Tránh retry quá nhiều khi service down
  - Mở circuit sau N lỗi liên tiếp
  - Half-open state để test recovery

**`dead_letter_queue.py`**
- **Nhiệm vụ:** Lưu failed tasks để retry sau
- **Chức năng:**
  - Lưu failed records vào Redis hoặc file
  - Có thể retry thủ công
  - Log chi tiết để debug

**`graceful_degradation.py`**
- **Nhiệm vụ:** Graceful degradation khi service down
- **Chức năng:**
  - Check service health
  - Degrade features nếu service down
  - Log degradation level

**`error_handler.py`**
- **Nhiệm vụ:** Tích hợp tất cả error handling components
- **Chức năng:**
  - Wrapper cho functions với error handling
  - Tự động retry, circuit breaker, DLQ
  - Logging và monitoring

### 1.8.2.3. Module `src/pipelines/crawl/storage/`

**Nhiệm vụ:** Quản lý storage và cache

**Các file:**

**`redis_cache.py`**
- **Nhiệm vụ:** Redis cache implementation
- **Chức năng:**
  - Get/Set cache với TTL
  - Batch operations
  - Connection pooling

**`multi_level_cache.py`**
- **Nhiệm vụ:** Multi-level cache (L1: Memory, L2: Redis, L3: File)
- **Chức năng:**
  - Cache hierarchy
  - Fallback mechanism
  - Cache invalidation

**`postgres_storage.py`**
- **Nhiệm vụ:** PostgreSQL storage operations
- **Chức năng:**
  - Connection management
  - Batch insert/upsert
  - Query helpers

**`compression.py`**
- **Nhiệm vụ:** Data compression utilities
- **Chức năng:**
  - Compress/decompress JSON files
  - Gzip support
  - Save disk space

### 1.8.2.4. Module `src/pipelines/transform/`

**Nhiệm vụ:** Transform và làm sạch dữ liệu

**File chính:**

**`transformer.py`**
- **Nhiệm vụ:** Transform dữ liệu từ raw sang processed
- **Input:** Raw JSON files
- **Output:** Transformed JSON files
- **Chức năng:**
  - Normalize text, numbers, dates
  - Validate schema và business rules
  - Flatten nested structures
  - Compute derived fields (revenue, popularity_score, value_score)

### 1.8.2.5. Module `src/pipelines/load/`

**Nhiệm vụ:** Load dữ liệu vào database

**File chính:**

**`loader.py`**
- **Nhiệm vụ:** Load dữ liệu vào PostgreSQL
- **Input:** Transformed JSON files
- **Output:** PostgreSQL database
- **Chức năng:**
  - Connect to PostgreSQL
  - Batch insert/upsert
  - Transaction management
  - Error handling và retry

### 1.8.2.6. Module `src/common/`

**Nhiệm vụ:** Common utilities được dùng chung

**Các file:**

**`config.py`**
- **Nhiệm vụ:** Common configuration
- **Chức năng:**
  - Environment variables
  - Database connections
  - Cache settings

**`ai/summarizer.py`**
- **Nhiệm vụ:** AI summarization cho notifications
- **Chức năng:**
  - Tóm tắt kết quả crawl
  - Generate insights
  - Format cho Discord notifications

**`analytics/aggregator.py`**
- **Nhiệm vụ:** Aggregate dữ liệu cho analytics
- **Chức năng:**
  - Tính toán KPI
  - Aggregate by category, brand, seller
  - Generate statistics

**`notifications/discord.py`**
- **Nhiệm vụ:** Gửi notifications qua Discord
- **Chức năng:**
  - Discord webhook integration
  - Format messages với Markdown
  - Send success/failure notifications

### 1.8.2.7. Module `airflow/dags/`

**Nhiệm vụ:** Airflow DAGs để orchestrate pipeline

**File chính:**

**`tiki_crawl_products_dag.py`**
- **Nhiệm vụ:** DAG chính để crawl sản phẩm Tiki
- **Chức năng:**
  - Định nghĩa workflow với TaskGroups
  - Dynamic Task Mapping cho parallel crawling
  - Error handling và retry
  - Notifications khi hoàn thành
  - Asset tracking

**Cấu trúc DAG:**
- **TaskGroup: `load_and_prepare`**
  - Load categories từ file
  - Prepare categories để crawl

- **TaskGroup: `crawl_categories`**
  - Dynamic Task Mapping: crawl products từ mỗi category

- **TaskGroup: `process_and_save`**
  - Merge products
  - Crawl details (Dynamic Task Mapping)
  - Transform và load vào database

- **TaskGroup: `validate`**
  - Validate data quality
  - Aggregate và notify

## 1.8.3. Luồng Dữ liệu giữa các Module

```
Tiki.vn
  ↓
crawl_categories_recursive.py
  ↓ (categories JSON)
crawl_products.py
  ↓ (products JSON)
crawl_products_detail.py
  ↓ (products_with_detail JSON)
transformer.py
  ↓ (products_transformed JSON)
loader.py
  ↓
PostgreSQL Database
  ↓
Data Mart (SQL Views)
  ↓
Dashboard (Power BI)
```

## 1.8.4. Dependencies giữa các Module

**Crawl Module:**
- Depends on: `utils.py`, `config.py`, `storage/redis_cache.py`
- Used by: Airflow DAG

**Transform Module:**
- Depends on: Crawl output (JSON files)
- Used by: Airflow DAG, Load module

**Load Module:**
- Depends on: Transform output (JSON files), `storage/postgres_storage.py`
- Used by: Airflow DAG

**Common Module:**
- Used by: All modules (config, notifications, analytics)

**Resilience Module:**
- Used by: Crawl module (error handling)

**Storage Module:**
- Used by: Crawl module (cache), Load module (database)

---

# 2. KIẾN TRÚC TỔNG THỂ (DATA PLATFORM ARCHITECTURE)

Kiến trúc của hệ thống được thiết kế theo mô hình **layered architecture** (kiến trúc phân lớp), mỗi layer có trách nhiệm riêng biệt và giao tiếp với nhau thông qua các interface chuẩn hóa. Thiết kế này đảm bảo tính **modularity** (mô đun hóa), **scalability** (khả năng mở rộng) và **maintainability** (dễ bảo trì).

## 2.1. Data Source Layer

### Nhiệm vụ

**Data Source Layer** là lớp đầu tiên trong kiến trúc, chịu trách nhiệm xác định và kết nối với các nguồn dữ liệu bên ngoài.

- **Kết nối với Tiki.vn:** Thực hiện HTTP/HTTPS requests hoặc Selenium WebDriver để truy cập dữ liệu
- **Xác định cấu trúc dữ liệu:** Hiểu cấu trúc HTML/JSON của website để extract dữ liệu
- **Xử lý authentication:** Nếu cần đăng nhập hoặc API key
- **Xử lý rate limiting:** Tuân thủ robots.txt và tránh bị block

### Dữ liệu Đi vào

- **Không có dữ liệu đi vào** (đây là điểm bắt đầu của pipeline)

### Dữ liệu Đi ra

- **Raw HTML/JSON:** Dữ liệu thô được crawl từ Tiki.vn
- **Categories:** Danh sách danh mục sản phẩm
- **Products:** Danh sách sản phẩm từ mỗi category
- **Product Details:** Chi tiết sản phẩm (price, rating, description, etc.)

### Công nghệ

- **Selenium WebDriver 4.0+:** Để crawl dynamic content (JavaScript-rendered)
- **BeautifulSoup4:** Để parse HTML tĩnh
- **Requests:** Để thực hiện HTTP requests
- **WebDriver Manager:** Để quản lý Chrome/Chromium driver

### Lý do Chọn

- **Reliability:** Selenium hỗ trợ xử lý JavaScript tốt, đảm bảo crawl được dữ liệu động
- **Scalability:** Có thể chạy nhiều crawler song song với Dynamic Task Mapping
- **Maintainability:** Dễ debug và monitor với logging rõ ràng

## 2.2. Ingestion Layer (Batch/Stream/Crawl/API)

### Nhiệm vụ

**Ingestion Layer** chịu trách nhiệm thu thập dữ liệu từ Data Source Layer và đưa vào hệ thống lưu trữ ban đầu (Raw Zone).

- **Orchestration:** Quản lý workflow crawl (categories → products → details)
- **Batch Processing:** Xử lý theo batch để tối ưu memory
- **Error Handling:** Xử lý lỗi và retry khi cần
- **Rate Limiting:** Điều chỉnh tốc độ crawl để tránh bị block
- **Caching:** Lưu cache để tránh crawl lại dữ liệu đã có

### Dữ liệu Đi vào

- **Metadata:** Thông tin về categories cần crawl
- **Configuration:** Các tham số crawl (timeout, delay, max pages)

### Dữ liệu Đi ra

- **Raw JSON Files:** Dữ liệu thô được lưu tại `data/raw/`
  - `categories_recursive_optimized.json`
  - `products/products.json`
  - `products/products_with_detail.json`
- **Asset/Dataset:** Tạo asset để trigger downstream tasks
  - `tiki://products/raw`
  - `tiki://products/with_detail`

### Công nghệ

- **Apache Airflow 3.1.2:** Workflow orchestration
- **Python:** Logic crawl và xử lý
- **Redis:** Multi-level cache
- **File System:** Lưu raw JSON files

### Lý do Chọn

- **Reliability:** Airflow có retry mechanism và error handling mạnh
- **Scalability:** Dynamic Task Mapping cho phép crawl song song nhiều categories/products
- **Maintainability:** DAG visualization giúp dễ hiểu và debug workflow

## 2.3. Raw Zone (Data Lake)

### Nhiệm vụ

**Raw Zone** là khu vực lưu trữ dữ liệu thô (raw data), không thay đổi dữ liệu gốc từ nguồn.

- **Lưu trữ nguyên bản:** Giữ lại dữ liệu gốc để có thể reprocess nếu cần
- **Versioning:** Có thể lưu nhiều version của dữ liệu (timestamp-based)
- **Audit Trail:** Theo dõi nguồn gốc và thời điểm thu thập

### Dữ liệu Đi vào

- **Raw data từ Ingestion Layer:** JSON files từ crawl

### Dữ liệu Đi ra

- **Raw data cho Cleaning Layer:** Dữ liệu để transform

### Công nghệ

- **File System:** JSON files trên disk
- **Compression:** Có thể nén file để tiết kiệm dung lượng
- **Backup:** Backup ra cloud storage (S3, GCS) nếu cần

### Lý do Chọn

- **Reliability:** File system đơn giản, ít lỗi
- **Scalability:** Có thể lưu trữ lượng lớn dữ liệu
- **Maintainability:** Dễ backup và restore

## 2.4. Cleansed Zone

### Nhiệm vụ

**Cleansed Zone** chịu trách nhiệm làm sạch và chuẩn hóa dữ liệu từ Raw Zone.

- **Data Cleaning:** Loại bỏ duplicate, missing values, invalid data
- **Normalization:** Chuẩn hóa format (text, numbers, dates)
- **Validation:** Kiểm tra dữ liệu theo business rules
- **Mapping:** Map dữ liệu về format chuẩn

### Dữ liệu Đi vào

- **Raw data từ Raw Zone:** JSON files

### Dữ liệu Đi ra

- **Cleansed data:** Dữ liệu đã được làm sạch, lưu tại `data/processed/`

### Công nghệ

- **Python DataTransformer:** Custom transformer class
- **Pandas (optional):** Để xử lý dữ liệu structured nếu cần

### Lý do Chọn

- **Reliability:** Có validation để đảm bảo chất lượng
- **Scalability:** Batch processing
- **Maintainability:** Code rõ ràng, dễ test

## 2.5. Transformed Zone

### Nhiệm vụ

**Transformed Zone** chịu trách nhiệm transform dữ liệu từ Cleansed Zone thành format phù hợp với Data Warehouse và tính toán các derived fields.

- **Data Transformation:** Chuyển đổi format để phù hợp với database schema
- **Computed Fields:** Tính toán các trường derived (revenue, popularity_score, value_score)
- **Enrichment:** Bổ sung dữ liệu từ nguồn khác nếu cần
- **Flattening:** Chuyển nested structures thành flat structure

### Dữ liệu Đi vào

- **Cleansed data từ Cleansed Zone**

### Dữ liệu Đi ra

- **Transformed data:** Dữ liệu đã transform, lưu tại `data/processed/products_transformed.json`
- **Asset:** `tiki://products/transformed`

### Công nghệ

- **Python DataTransformer:** Custom transformer với business logic
- **JSON:** Format trung gian

### Lý do Chọn

- **Reliability:** Business logic rõ ràng, dễ test
- **Scalability:** Có thể tối ưu hiệu suất
- **Maintainability:** Code modular, dễ extend

## 2.6. Data Warehouse / Lakehouse

### Nhiệm vụ

**Data Warehouse** là nơi lưu trữ dữ liệu đã được chuẩn hóa theo mô hình relational, sẵn sàng cho query và phân tích.

- **Schema Design:** Thiết kế schema theo 3NF hoặc Star Schema
- **Indexing:** Tạo index để tối ưu query
- **Partitioning:** Partition theo thời gian nếu cần
- **Data Quality:** Đảm bảo integrity constraints

### Dữ liệu Đi vào

- **Transformed data từ Transformed Zone**

### Dữ liệu Đi ra

- **Structured data:** Dữ liệu trong PostgreSQL tables
- **Asset:** `tiki://products/final`

### Công nghệ

- **PostgreSQL 16:** Relational database
- **Connection Pooling:** Tối ưu kết nối database
- **JSONB:** Lưu dữ liệu semi-structured (specifications, images)

### Lý do Chọn

- **Reliability:** ACID compliance, transaction support
- **Scalability:** Có thể scale với read replicas
- **Maintainability:** SQL queries dễ viết và debug

## 2.7. Analytics Layer (BI Dashboard, SQL Transform, Data Mart)

### Nhiệm vụ

**Analytics Layer** cung cấp các công cụ và môi trường để phân tích dữ liệu từ Data Warehouse.

- **SQL Transform:** Viết SQL queries để tổng hợp và transform dữ liệu
- **Data Mart:** Tạo các data mart theo chủ đề (products, sales, pricing)
- **KPI Calculation:** Tính toán các chỉ số KPI
- **Ad-hoc Analysis:** Hỗ trợ phân tích ad-hoc

### Dữ liệu Đi vào

- **Structured data từ Data Warehouse**

### Dữ liệu Đi ra

- **Aggregated data:** Dữ liệu tổng hợp cho BI
- **KPI Metrics:** Các chỉ số đã tính toán

### Công nghệ

- **SQL:** Query language
- **PostgreSQL:** Database engine
- **Optional:** Spark, Dask cho big data nếu cần

### Lý do Chọn

- **Reliability:** SQL đã được kiểm chứng
- **Scalability:** Có thể tối ưu queries
- **Maintainability:** SQL dễ đọc và maintain

## 2.8. Serving Layer (Report, API, Dashboard)

### Nhiệm vụ

**Serving Layer** cung cấp interface để người dùng cuối truy cập và xem dữ liệu.

- **BI Dashboard:** Trực quan hóa dữ liệu với charts, tables
- **Reports:** Tạo báo cáo tự động
- **API:** Cung cấp REST API để truy cập dữ liệu (nếu cần)
- **Export:** Export dữ liệu ra Excel, CSV, PDF

### Dữ liệu Đi vào

- **Aggregated data từ Analytics Layer**

### Dữ liệu Đi ra

- **Visualizations:** Charts, graphs, tables
- **Reports:** PDF, Excel reports
- **API Responses:** JSON data

### Công nghệ

- **Power BI / Superset / Tableau:** BI tools
- **Optional:** FastAPI, Flask cho REST API
- **Optional:** Jupyter Notebook cho ad-hoc analysis

### Lý do Chọn

- **Reliability:** Tools đã được sử dụng rộng rãi
- **Scalability:** Có thể cache queries
- **Maintainability:** Tools có UI, dễ sử dụng

## 2.9. Monitoring & Logging Layer

### Nhiệm vụ

**Monitoring & Logging Layer** theo dõi toàn bộ hệ thống để đảm bảo hoạt động ổn định.

- **Pipeline Monitoring:** Theo dõi trạng thái DAG, tasks
- **Data Quality Monitoring:** Kiểm tra chất lượng dữ liệu (freshness, completeness, accuracy)
- **Performance Monitoring:** Theo dõi hiệu suất (execution time, resource usage)
- **Error Tracking:** Ghi log và alert khi có lỗi

### Dữ liệu Đi vào

- **Logs từ các layers**
- **Metrics từ các components**

### Dữ liệu Đi ra

- **Alerts:** Thông báo khi có vấn đề
- **Dashboards:** Monitoring dashboards
- **Reports:** Weekly/monthly reports

### Công nghệ

- **Airflow UI:** Built-in monitoring
- **Logging:** Python logging module
- **Optional:** Prometheus, Grafana cho advanced monitoring
- **Optional:** ELK Stack cho log aggregation

### Lý do Chọn

- **Reliability:** Đảm bảo phát hiện lỗi sớm
- **Scalability:** Có thể scale monitoring infrastructure
- **Maintainability:** Centralized logging và monitoring

---

# 3. LUỒNG DỮ LIỆU END-TO-END (DE + DA WORKFLOW)

Phần này mô tả toàn bộ **story** của dữ liệu từ khi xuất hiện ở nguồn cho đến khi hiển thị trên dashboard cho người dùng cuối.

## 3.1. Dữ liệu Xuất hiện từ Đâu

Dữ liệu bắt đầu từ **Tiki.vn website**, một nền tảng thương mại điện tử lớn tại Việt Nam. Website này lưu trữ:

- **Categories:** Hàng nghìn danh mục sản phẩm được tổ chức theo cấu trúc phân cấp
- **Products:** Hàng triệu sản phẩm được phân loại vào các danh mục
- **Product Details:** Thông tin chi tiết của từng sản phẩm (giá, đánh giá, mô tả, v.v.)

Dữ liệu trên website là **dynamic content** (nội dung động), được render bởi JavaScript, do đó không thể crawl đơn giản bằng HTTP requests mà cần sử dụng Selenium WebDriver.

## 3.2. Cách Thu thập (Ingestion)

### 3.2.1. Crawl Categories (Đệ quy)

**Quy trình:**
1. Bắt đầu từ trang chủ Tiki.vn hoặc trang danh mục
2. Parse HTML để lấy danh sách danh mục con (sub-categories)
3. Với mỗi danh mục con, đệ quy crawl các danh mục con của nó
4. Lưu thông tin: name, slug, URL, parent_url, level

**Output:**
- File: `data/raw/categories_recursive_optimized.json`
- Format: JSON array of category objects

### 3.2.2. Crawl Products (Từ Categories)

**Quy trình:**
1. Với mỗi category URL, crawl danh sách sản phẩm
2. Xử lý phân trang: crawl từng trang sản phẩm (page 1, 2, 3, ...)
3. Parse HTML để extract thông tin cơ bản: product_id, name, URL, image_url, sales_count
4. Lưu kèm category_url để biết sản phẩm thuộc danh mục nào

**Output:**
- File: `data/raw/products/products.json`
- Format: JSON array of product objects
- Asset: `tiki://products/raw`

### 3.2.3. Crawl Product Details (Selenium)

**Quy trình:**
1. Với mỗi product URL, sử dụng Selenium WebDriver để load trang chi tiết
2. Đợi JavaScript render hoàn tất
3. Extract thông tin chi tiết:
   - Price (current_price, original_price, discount_percent)
   - Rating (average, total_reviews)
   - Description
   - Specifications (JSON)
   - Images (array)
   - Seller information
   - Brand
   - Stock status
   - Shipping options

**Output:**
- File: `data/raw/products/products_with_detail.json`
- Format: JSON array of product objects with full details
- Asset: `tiki://products/with_detail`

**Tối ưu:**
- Sử dụng Redis cache để tránh crawl lại sản phẩm đã có
- Rate limiting (delay 2-3 giây giữa các requests) để tránh bị block
- Batch processing để xử lý nhiều sản phẩm cùng lúc

## 3.3. Cách Lưu trữ Dữ liệu Thô (Raw)

Dữ liệu thô được lưu tại **Raw Zone** dưới dạng JSON files:

**Cấu trúc thư mục:**
```
data/raw/
├── categories_recursive_optimized.json
├── categories_tree.json
└── products/
    ├── products.json
    └── products_with_detail.json
```

**Đặc điểm:**
- **Format:** JSON (human-readable, dễ debug)
- **Versioning:** Có thể lưu với timestamp (ví dụ: `products_2024-01-01.json`)
- **Compression:** Có thể nén (gzip) để tiết kiệm dung lượng
- **Backup:** Backup ra cloud storage (S3, GCS) nếu cần

## 3.4. Cách Làm sạch, Chuẩn hóa (Cleaning)

### 3.4.1. Normalization

**Text Normalization:**
- Trim whitespace
- Remove special characters không cần thiết
- Convert encoding (UTF-8)
- Normalize brand name (loại bỏ "Thương hiệu: " prefix)

**Number Normalization:**
- Parse string thành int/float
- Loại bỏ ký tự không phải số (ví dụ: "1,000" → 1000)
- Xử lý null/empty values

**Date Normalization:**
- Parse nhiều format date → ISO format
- Timezone: Asia/Ho_Chi_Minh

### 3.4.2. Validation

**Schema Validation:**
- Kiểm tra required fields (product_id, name, url)
- Kiểm tra format (product_id phải là số, URL phải bắt đầu bằng http/https)

**Business Rules Validation:**
- Price không được âm
- Current price không được lớn hơn original price
- Rating phải trong khoảng 0-5
- Sales count không được âm

**Duplicate Detection:**
- Kiểm tra duplicate theo product_id
- Loại bỏ duplicate (giữ record mới nhất)

### 3.4.3. Missing Value Handling

- **NULL vs Empty String:** Chuẩn hóa về NULL hoặc empty string tùy context
- **Default Values:** Gán giá trị mặc định nếu cần (ví dụ: stock_available = False nếu không có thông tin)
- **Imputation:** Có thể impute giá trị nếu có logic nghiệp vụ (ví dụ: giá trung bình của category)

## 3.5. Cách Transform theo Mô hình 3NF hoặc Kimball

### 3.5.1. Flattening (Chuyển Nested → Flat)

**Trước khi transform:**
```json
{
  "product_id": "123",
  "price": {
    "current_price": 100000,
    "original_price": 150000,
    "discount_percent": 33.3
  },
  "rating": {
    "average": 4.5,
    "total_reviews": 100
  }
}
```

**Sau khi transform:**
```json
{
  "product_id": "123",
  "price": 100000,
  "original_price": 150000,
  "discount_percent": 33,
  "rating_average": 4.5,
  "review_count": 100
}
```

### 3.5.2. Computed Fields

Tính toán các trường derived:

- **estimated_revenue = sales_count × price**
- **price_savings = original_price - price**
- **price_category:** Phân loại theo giá (budget/mid-range/premium/luxury)
- **popularity_score:** Điểm từ 0-100 dựa trên sales_count, rating, review_count
- **value_score:** Điểm giá trị = rating / (price / 1M)
- **discount_amount = original_price - price**
- **sales_velocity = sales_count** (có thể tính chi tiết hơn nếu có dữ liệu theo thời gian)

### 3.5.3. Database Schema Mapping

Transform để phù hợp với PostgreSQL schema:

- Flatten nested structures
- Map fields: `rating.total_reviews` → `review_count`
- JSONB fields: `specifications`, `images`, `shipping` giữ nguyên dạng JSON
- Timestamps: `crawled_at` → ISO format string

## 3.6. Cách Đưa vào Data Warehouse

### 3.6.1. Load vào PostgreSQL

**Process:**
1. Kết nối với PostgreSQL database (`crawl_data`)
2. Batch insert/upsert vào table `products`
3. Sử dụng `ON CONFLICT (product_id) DO UPDATE` để upsert
4. Transaction để đảm bảo atomicity

**Schema:**
- Table: `products` (xem chi tiết ở phần Data Model)
- Indexes: `product_id`, `category_url`, `sales_count`, `crawled_at`

### 3.6.2. Backup JSON

Lưu dữ liệu đã transform vào JSON file để backup:
- File: `data/processed/products_final.json`
- Format: JSON với metadata (loaded_at, total_products, stats)

## 3.7. Cách Tạo Data Mart cho BI

### 3.7.1. Products Mart

**Tables/Views:**
- `products_mart`: View tổng hợp sản phẩm với các metrics
  - Columns: product_id, name, category, price, sales_count, revenue, popularity_score, value_score
  - Aggregations: GROUP BY category để có category-level metrics

### 3.7.2. Sales Mart

**Tables/Views:**
- `sales_mart`: View về doanh số
  - Columns: date, category, total_revenue, total_sales_count, avg_price
  - Time-based aggregations (daily, weekly, monthly)

### 3.7.3. Pricing Mart

**Tables/Views:**
- `pricing_mart`: View về giá cả
  - Columns: category, price_range, avg_price, min_price, max_price, discount_percent

## 3.8. Cách Data Analyst Phân tích Dữ liệu

### 3.8.1. SQL Queries

**Ví dụ queries:**

**Top 10 sản phẩm bán chạy nhất:**
```sql
SELECT name, sales_count, estimated_revenue
FROM products
ORDER BY sales_count DESC
LIMIT 10;
```

**Phân tích theo category:**
```sql
SELECT 
  category_url,
  COUNT(*) as product_count,
  AVG(price) as avg_price,
  SUM(estimated_revenue) as total_revenue
FROM products
GROUP BY category_url
ORDER BY total_revenue DESC;
```

**Sản phẩm có giá trị tốt nhất (value_score cao):**
```sql
SELECT name, price, rating_average, value_score
FROM products
WHERE value_score IS NOT NULL
ORDER BY value_score DESC
LIMIT 20;
```

### 3.8.2. Ad-hoc Analysis

- **Jupyter Notebook:** Tạo notebook để phân tích chi tiết
- **Python/Pandas:** Xử lý dữ liệu, tính toán statistics
- **Visualization:** Matplotlib, Seaborn, Plotly để vẽ biểu đồ

## 3.9. Cách Dashboard Hiển thị Cuối cùng cho Người dùng

### 3.9.1. Dashboard Overview

**Các components:**
- **KPI Cards:** Tổng số sản phẩm, tổng doanh thu, trung bình rating
- **Charts:**
  - Bar chart: Top 10 sản phẩm bán chạy
  - Line chart: Xu hướng giá theo thời gian
  - Pie chart: Phân bố sản phẩm theo category
  - Map (nếu có dữ liệu địa lý): Heatmap theo khu vực

### 3.9.2. Dashboard Detail

- **Product Detail Page:** Chi tiết từng sản phẩm
- **Category Detail Page:** Phân tích theo danh mục
- **Drill-down:** Click vào category → xem danh sách sản phẩm trong category

### 3.9.3. Reports

- **Daily Report:** Báo cáo hàng ngày về sản phẩm mới, giá thay đổi
- **Weekly Report:** Tổng hợp tuần
- **Monthly Report:** Phân tích xu hướng tháng

## 3.10. Luồng Phản hồi nếu Có Lỗi (Error Flow)

### 3.10.1. Error Detection

**Các loại lỗi:**
- **Crawl errors:** Website không accessible, timeout, rate limit
- **Parse errors:** HTML structure thay đổi, không parse được
- **Validation errors:** Dữ liệu không pass validation
- **Database errors:** Connection failed, constraint violation

### 3.10.2. Error Handling

**Retry Mechanism:**
- Airflow retry: Tự động retry 3 lần nếu task fail
- Exponential backoff: Tăng delay giữa các retry

**Error Logging:**
- Ghi log chi tiết vào Airflow logs
- Lưu failed records vào dead letter queue (DLQ)
- Gửi alert qua Discord/Slack/Email

**Graceful Degradation:**
- Nếu 1 category fail → tiếp tục crawl category khác
- Nếu 1 product fail → skip và tiếp tục product khác
- Nếu detail crawl fail → vẫn lưu thông tin cơ bản

### 3.10.3. Recovery

- **Manual Retry:** Quản trị viên có thể retry failed tasks từ Airflow UI
- **Reprocess:** Có thể reprocess từ Raw Zone nếu cần
- **Partial Success:** Lưu dữ liệu đã crawl được, sau đó retry phần fail

## 3.11. Luồng Cập nhật Nâng cấp (Versioning, Incremental Load)

### 3.11.1. Versioning

**Dữ liệu Raw:**
- Lưu file với timestamp: `products_2024-01-01_120000.json`
- Giữ lại nhiều version để có thể rollback

**Database:**
- Column `updated_at` để track thời gian cập nhật
- Column `crawled_at` để track thời gian crawl
- Có thể tạo table `products_history` để lưu lịch sử thay đổi

### 3.11.2. Incremental Load

**Strategy:**
- **Full Load:** Lần đầu chạy full crawl tất cả sản phẩm
- **Incremental Load:** Các lần sau chỉ crawl sản phẩm mới hoặc đã thay đổi

**Cách xác định sản phẩm mới/thay đổi:**
- So sánh `product_id` với database
- Nếu chưa có → INSERT
- Nếu đã có → UPDATE nếu có thay đổi (so sánh hash hoặc timestamp)

**Tối ưu:**
- Chỉ crawl detail cho sản phẩm mới hoặc đã thay đổi
- Sử dụng cache để tránh crawl lại sản phẩm không đổi

### 3.11.3. Schema Evolution

- **Add new columns:** Sử dụng `ALTER TABLE ADD COLUMN IF NOT EXISTS`
- **Migration scripts:** Tạo migration scripts để update schema
- **Backward compatibility:** Đảm bảo code cũ vẫn hoạt động với schema mới

---

# 4. QUY TRÌNH ETL / ELT CHI TIẾT

Phần này mô tả chi tiết quy trình ETL (Extract, Transform, Load) được áp dụng trong dự án. Hệ thống sử dụng mô hình **ELT** (Extract → Load → Transform) với việc load dữ liệu thô vào Raw Zone trước, sau đó transform khi cần.

## 4.1. Ingestion (Source → Raw)

### 4.1.1. Extract Categories

**Input:** Tiki.vn website  
**Output:** `data/raw/categories_recursive_optimized.json`

**Quy trình:**
1. **Bắt đầu từ root categories:** Lấy danh sách danh mục cấp 1 từ trang chủ
2. **Đệ quy crawl:** Với mỗi category, crawl các sub-categories
3. **Depth control:** Giới hạn độ sâu (level) để tránh crawl quá sâu
4. **Deduplication:** Loại bỏ duplicate categories (dựa trên URL)

**Output format:**
```json
{
  "name": "Điện thoại",
  "slug": "dien-thoai",
  "url": "https://tiki.vn/dien-thoai/c1789",
  "image_url": "...",
  "parent_url": "https://tiki.vn/",
  "level": 1
}
```

### 4.1.2. Extract Products

**Input:** Category URLs  
**Output:** `data/raw/products/products.json`

**Quy trình:**
1. **Với mỗi category URL:**
   - Crawl trang 1 để lấy tổng số trang
   - Crawl từng trang sản phẩm (page 1, 2, ..., max_pages)
2. **Parse HTML:** Extract thông tin cơ bản từ HTML
   - product_id, name, URL, image_url, sales_count
3. **Rate limiting:** Delay 1-2 giây giữa các requests
4. **Error handling:** Nếu 1 category fail → log và tiếp tục category khác

**Output format:**
```json
{
  "product_id": "123456789",
  "name": "iPhone 15 Pro Max",
  "url": "https://tiki.vn/iphone-15-pro-max-p123456789.html",
  "image_url": "...",
  "sales_count": 2000,
  "category_url": "https://tiki.vn/dien-thoai/c1789",
  "crawled_at": "2024-01-01T12:00:00"
}
```

### 4.1.3. Extract Product Details

**Input:** Product URLs  
**Output:** `data/raw/products/products_with_detail.json`

**Quy trình:**
1. **Sử dụng Selenium WebDriver:**
   - Load trang chi tiết sản phẩm
   - Đợi JavaScript render (wait for element)
   - Extract dữ liệu từ DOM
2. **Cache mechanism:**
   - Kiểm tra Redis cache trước
   - Nếu đã có → skip crawl
   - Nếu chưa có → crawl và lưu cache
3. **Extract fields:**
   - Price (current, original, discount)
   - Rating (average, total_reviews)
   - Description, Specifications (JSON)
   - Images (array), Seller, Brand, Stock

**Output format:**
```json
{
  "product_id": "123456789",
  "name": "iPhone 15 Pro Max",
  "price": {
    "current_price": 28990000,
    "original_price": 32990000,
    "discount_percent": 12.13
  },
  "rating": {
    "average": 4.5,
    "total_reviews": 150
  },
  "description": "...",
  "specifications": {...},
  "images": [...],
  "brand": "Apple",
  "seller": {
    "name": "Tiki Trading",
    "seller_id": "123",
    "is_official": true
  },
  "stock": {
    "available": true,
    "quantity": 10,
    "status": "in_stock"
  }
}
```

## 4.2. Validation (Schema, Duplicate, Anomaly)

### 4.2.1. Schema Validation

**Kiểm tra cấu trúc dữ liệu:**

**Categories:**
- Required fields: `name`, `url`
- Optional fields: `slug`, `image_url`, `parent_url`, `level`
- URL format: Phải bắt đầu bằng `https://tiki.vn/`

**Products:**
- Required fields: `product_id`, `name`, `url`
- Product ID format: Chỉ chứa số
- URL format: Phải bắt đầu bằng `http://` hoặc `https://`

**Product Details:**
- Price validation: `current_price <= original_price`, giá không âm
- Rating validation: `0 <= rating_average <= 5`
- Review count: `review_count >= 0`

### 4.2.2. Duplicate Detection

**Phương pháp:**
- **Categories:** Dựa trên `url` (UNIQUE constraint)
- **Products:** Dựa trên `product_id` (UNIQUE constraint)

**Xử lý:**
- Nếu duplicate → giữ record mới nhất (dựa trên `crawled_at` hoặc `updated_at`)
- Log duplicate để theo dõi

### 4.2.3. Anomaly Detection

**Các loại anomaly:**
- **Price anomaly:** Giá quá cao/thấp so với trung bình category
- **Sales anomaly:** Sales count tăng/giảm đột ngột
- **Rating anomaly:** Rating quá cao/thấp so với review_count

**Xử lý:**
- Log anomaly để review thủ công
- Có thể flag record với `anomaly_flag = true`
- Không tự động loại bỏ (có thể là dữ liệu hợp lệ)

## 4.3. Cleaning (Missing Value, Normalize Format, Mapping)

### 4.3.1. Missing Value Handling

**Strategies:**
- **NULL vs Empty String:**
  - Text fields: NULL nếu không có giá trị
  - Numeric fields: NULL nếu không parse được
  - Boolean fields: FALSE (default)

- **Imputation (nếu cần):**
  - Price: Có thể lấy giá trung bình của category (nhưng trong dự án này KHÔNG impute, để NULL)
  - Rating: Không impute, để NULL

### 4.3.2. Normalize Format

**Text Normalization:**
```python
def normalize_text(text: str) -> str:
    # Trim whitespace
    text = text.strip()
    # Remove extra spaces
    text = " ".join(text.split())
    return text
```

**Number Normalization:**
```python
def parse_int(value: Any) -> int | None:
    # Remove non-digit characters
    cleaned = re.sub(r"[^\d]", "", str(value))
    return int(cleaned) if cleaned else None
```

**Date Normalization:**
```python
def parse_datetime(value: Any) -> datetime | None:
    # Try multiple formats
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d"
    ]
    # Parse and convert to ISO format
    return datetime.strptime(value, fmt).isoformat()
```

**Brand Normalization:**
- Loại bỏ prefix "Thương hiệu: "
- Trim whitespace
- Title case nếu cần

### 4.3.3. Mapping

**Category URL → Category ID:**
- Extract category_id từ URL: `/c1789` → `c1789`
- Map với categories table nếu cần

**Seller Name → Seller ID:**
- Normalize seller name
- Map với sellers table nếu có

## 4.4. Transformation

### 4.4.1. Chuẩn hóa Dữ liệu về 3NF

**3NF (Third Normal Form):**
- **Loại bỏ redundancy:** Không lặp lại dữ liệu
- **Functional dependencies:** Mỗi non-key attribute phụ thuộc vào primary key

**Trong dự án này:**

**Table: `products`**
- Primary key: `product_id`
- All attributes phụ thuộc vào `product_id`
- Không có transitive dependencies

**Table: `categories`** (nếu tách riêng)
- Primary key: `category_id` hoặc `url`
- Attributes: name, slug, image_url, parent_url, level

**Normalization:**
- Products table: Lưu `category_url` (denormalized) để dễ query, nhưng có thể join với categories nếu cần
- Seller fields: Flatten vào products table (có thể tách thành sellers table nếu cần)
- Brand: Lưu trong products (có thể tách thành brands table nếu cần)

### 4.4.2. Xây Data Warehouse theo Star Schema

**Star Schema Design:**

**Fact Table: `products` (denormalized fact table)**
- Grain: 1 row = 1 product snapshot tại 1 thời điểm
- Measures:
  - `sales_count` (số lượng đã bán)
  - `estimated_revenue` (doanh thu ước tính)
  - `price`, `original_price` (giá)
  - `popularity_score`, `value_score` (metrics)

**Dimension Tables (conceptual, có thể tách nếu cần):**

**Dim_Category:**
- category_id (surrogate key)
- category_name
- category_url
- parent_category_id
- category_level

**Dim_Seller:**
- seller_id (surrogate key)
- seller_name
- seller_is_official

**Dim_Brand:**
- brand_id (surrogate key)
- brand_name

**Dim_Time:** (nếu cần phân tích theo thời gian)
- date_key
- date
- day, month, year
- quarter, week

**Trong implementation:**
- Hiện tại: Products table chứa tất cả (denormalized) để đơn giản
- Future: Có thể normalize thành Star Schema nếu cần scale hoặc optimize

### 4.4.3. Xây Data Mart phục vụ Dashboard

**Data Marts:**

**1. Products Mart:**
```sql
CREATE VIEW products_mart AS
SELECT 
    product_id,
    name,
    category_url,
    price,
    original_price,
    discount_percent,
    sales_count,
    estimated_revenue,
    rating_average,
    review_count,
    popularity_score,
    value_score,
    price_category,
    brand,
    seller_name,
    seller_is_official,
    crawled_at
FROM products
WHERE sales_count > 0  -- Chỉ lấy sản phẩm đã bán
ORDER BY sales_count DESC;
```

**2. Sales Mart:**
```sql
CREATE VIEW sales_mart AS
SELECT 
    category_url,
    COUNT(*) as product_count,
    SUM(sales_count) as total_sales,
    SUM(estimated_revenue) as total_revenue,
    AVG(price) as avg_price,
    AVG(discount_percent) as avg_discount,
    AVG(rating_average) as avg_rating
FROM products
WHERE sales_count > 0
GROUP BY category_url
ORDER BY total_revenue DESC;
```

**3. Pricing Mart:**
```sql
CREATE VIEW pricing_mart AS
SELECT 
    price_category,
    COUNT(*) as product_count,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price,
    AVG(discount_percent) as avg_discount
FROM products
WHERE price IS NOT NULL
GROUP BY price_category
ORDER BY avg_price;
```

## 4.5. Load vào Warehouse

### 4.5.1. Batch Insert/Upsert

**Process:**
1. **Connection Pooling:** Sử dụng connection pool để tối ưu kết nối
2. **Batch Size:** Insert theo batch (default: 100 records/batch)
3. **Upsert Logic:**
   ```sql
   INSERT INTO products (...)
   VALUES (...)
   ON CONFLICT (product_id)
   DO UPDATE SET
       name = EXCLUDED.name,
       price = EXCLUDED.price,
       ...
       updated_at = CURRENT_TIMESTAMP;
   ```
4. **Transaction:** Wrap trong transaction để đảm bảo atomicity

### 4.5.2. Error Handling

- **Database Connection Error:** Retry với exponential backoff
- **Constraint Violation:** Log và skip record (hoặc update nếu upsert)
- **Timeout:** Set timeout cho queries (30s default)

## 4.6. Partition, Indexing, Clustering

### 4.6.1. Indexing

**Indexes hiện tại:**

```sql
-- Primary key index (automatic)
CREATE INDEX idx_products_product_id ON products(product_id);

-- Foreign key / Join index
CREATE INDEX idx_products_category_url ON products(category_url);

-- Filter/Order index
CREATE INDEX idx_products_sales_count ON products(sales_count);

-- Time-based index
CREATE INDEX idx_products_crawled_at ON products(crawled_at);

-- Additional indexes (nếu cần)
CREATE INDEX idx_products_seller_id ON products(seller_id);
CREATE INDEX idx_products_brand ON products(brand);
CREATE INDEX idx_products_price_category ON products(price_category);

-- Composite indexes (nếu query thường dùng)
CREATE INDEX idx_products_category_sales ON products(category_url, sales_count);
```

### 4.6.2. Partitioning (Future)

**Nếu cần scale:**
- **Range Partitioning:** Partition theo `crawled_at` (monthly/quarterly)
- **List Partitioning:** Partition theo `category_url` nếu có ít categories

**Ví dụ:**
```sql
CREATE TABLE products_2024_01 PARTITION OF products
FOR VALUES FROM ('2024-01-01') TO ('2024-02-01');
```

### 4.6.3. Clustering

- **Clustered Index:** PostgreSQL không có clustered index như SQL Server, nhưng có thể sử dụng `CLUSTER` command
- **Order by:** Sắp xếp table theo `product_id` hoặc `category_url` để tối ưu queries thường dùng

## 4.7. Tối ưu Chi phí Lưu trữ và Tốc độ Truy vấn

### 4.7.1. Storage Optimization

- **Compression:** PostgreSQL tự động compress data
- **JSONB:** Sử dụng JSONB thay vì JSON để có index và query tốt hơn
- **Archive old data:** Move dữ liệu cũ (> 1 năm) ra archive table hoặc cold storage

### 4.7.2. Query Optimization

- **Use Indexes:** Đảm bảo queries sử dụng indexes
- **Avoid SELECT *:** Chỉ select columns cần thiết
- **Limit Results:** Sử dụng LIMIT khi query
- **Analyze Queries:** Sử dụng EXPLAIN ANALYZE để tối ưu queries

### 4.7.3. Connection Pooling

- **Connection Pool:** Sử dụng connection pool (SQLAlchemy, psycopg2.pool) để giảm overhead
- **Max Connections:** Giới hạn số connections đồng thời

---

# 5. DATA MODEL (ERD + STAR SCHEMA + DATA MART)

Phần này mô tả chi tiết về data model được sử dụng trong hệ thống, bao gồm ERD (Entity Relationship Diagram) cho hệ thống giao dịch gốc, Star Schema cho Data Warehouse, và Data Mart phục vụ phân tích.

## 5.1. ERD cho Hệ thống Giao dịch Gốc

### 5.1.1. Các Bảng

**1. Table: `categories`**

**Mục đích:** Lưu trữ thông tin danh mục sản phẩm từ Tiki.vn

| Column | Type | Constraints | Mô tả |
|--------|------|-------------|-------|
| id | SERIAL | PRIMARY KEY | Surrogate key tự động tăng |
| category_id | VARCHAR(255) | UNIQUE | Category ID từ Tiki (ví dụ: c1789) |
| name | VARCHAR(500) | NOT NULL | Tên danh mục |
| url | TEXT | NOT NULL, UNIQUE | URL danh mục |
| image_url | TEXT | | URL hình ảnh danh mục |
| parent_url | TEXT | | URL danh mục cha |
| level | INTEGER | | Cấp độ danh mục (1, 2, 3, ...) |
| product_count | INTEGER | DEFAULT 0 | Số lượng sản phẩm trong danh mục |
| created_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Thời gian tạo |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Thời gian cập nhật |

**Indexes:**
- `idx_categories_url` ON (url)
- `idx_categories_parent_url` ON (parent_url)
- `idx_categories_level` ON (level)

**2. Table: `products`**

**Mục đích:** Lưu trữ thông tin sản phẩm từ Tiki.vn

| Column | Type | Constraints | Mô tả |
|--------|------|-------------|-------|
| id | SERIAL | PRIMARY KEY | Surrogate key |
| product_id | VARCHAR(255) | UNIQUE, NOT NULL | Product ID từ Tiki |
| name | VARCHAR(1000) | NOT NULL | Tên sản phẩm |
| url | TEXT | NOT NULL | URL sản phẩm |
| image_url | TEXT | | URL hình ảnh chính |
| category_url | TEXT | | URL danh mục (FK to categories.url) |
| category_id | VARCHAR(255) | | Category ID (FK to categories.category_id) |
| category_path | JSONB | | Path danh mục (array) |
| sales_count | INTEGER | | Số lượng đã bán |
| price | DECIMAL(12, 2) | | Giá hiện tại (VND) |
| original_price | DECIMAL(12, 2) | | Giá gốc (VND) |
| discount_percent | INTEGER | | Phần trăm giảm giá |
| rating_average | DECIMAL(3, 2) | | Rating trung bình (0-5) |
| review_count | INTEGER | | Số lượng review |
| description | TEXT | | Mô tả sản phẩm |
| specifications | JSONB | | Thông số kỹ thuật (JSON) |
| images | JSONB | | Danh sách hình ảnh (JSON array) |
| seller_name | VARCHAR(500) | | Tên người bán |
| seller_id | VARCHAR(255) | | ID người bán |
| seller_is_official | BOOLEAN | DEFAULT FALSE | Có phải seller chính thức |
| brand | VARCHAR(255) | | Thương hiệu |
| stock_available | BOOLEAN | | Còn hàng không |
| stock_quantity | INTEGER | | Số lượng tồn kho |
| stock_status | VARCHAR(50) | | Trạng thái tồn kho |
| shipping | JSONB | | Thông tin vận chuyển (JSON) |
| estimated_revenue | DECIMAL(15, 2) | | Doanh thu ước tính (computed) |
| price_savings | DECIMAL(12, 2) | | Số tiền tiết kiệm (computed) |
| price_category | VARCHAR(50) | | Phân loại giá (computed) |
| popularity_score | DECIMAL(10, 2) | | Điểm độ phổ biến (computed) |
| value_score | DECIMAL(10, 2) | | Điểm giá trị (computed) |
| discount_amount | DECIMAL(12, 2) | | Số tiền giảm (computed) |
| sales_velocity | INTEGER | | Tốc độ bán (computed) |
| crawled_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Thời gian crawl |
| updated_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Thời gian cập nhật |

**Indexes:**
- `idx_products_product_id` ON (product_id)
- `idx_products_category_url` ON (category_url)
- `idx_products_category_id` ON (category_id)
- `idx_products_sales_count` ON (sales_count)
- `idx_products_crawled_at` ON (crawled_at)
- `idx_products_seller_id` ON (seller_id)
- `idx_products_brand` ON (brand)
- `idx_products_price_category` ON (price_category)
- `idx_products_category_path` ON (category_path) USING GIN

**3. Table: `crawl_history`**

**Mục đích:** Theo dõi lịch sử crawl để monitoring và debug

| Column | Type | Constraints | Mô tả |
|--------|------|-------------|-------|
| id | SERIAL | PRIMARY KEY | Surrogate key |
| crawl_type | VARCHAR(50) | NOT NULL | Loại crawl (categories/products/detail) |
| category_url | TEXT | | URL category (nếu crawl products) |
| product_id | VARCHAR(255) | | Product ID (nếu crawl detail) |
| status | VARCHAR(20) | NOT NULL | Trạng thái (success/failed/partial) |
| items_count | INTEGER | DEFAULT 0 | Số lượng items đã crawl |
| error_message | TEXT | | Thông báo lỗi (nếu có) |
| started_at | TIMESTAMP | DEFAULT CURRENT_TIMESTAMP | Thời gian bắt đầu |
| completed_at | TIMESTAMP | | Thời gian hoàn thành |

**Indexes:**
- `idx_crawl_history_type` ON (crawl_type)
- `idx_crawl_history_started_at` ON (started_at)

### 5.1.2. Khóa Chính, Khóa Ngoại

**Primary Keys:**
- `categories.id` (surrogate key)
- `products.id` (surrogate key)
- `crawl_history.id` (surrogate key)

**Unique Keys:**
- `categories.category_id` (UNIQUE)
- `categories.url` (UNIQUE)
- `products.product_id` (UNIQUE)

**Foreign Keys (Conceptual):**
- `products.category_url` → `categories.url`
- `products.category_id` → `categories.category_id`

**Lưu ý:** Trong implementation hiện tại, không có explicit FOREIGN KEY constraints để tránh ràng buộc khi crawl (có thể product có category_url nhưng category chưa được crawl). Tuy nhiên, có thể thêm constraints sau khi đảm bảo data consistency.

### 5.1.3. Mối quan hệ

**1. Categories ↔ Products (1-N):**
- 1 category có nhiều products
- 1 product thuộc 1 category (hoặc NULL nếu chưa phân loại)
- **Relationship:** `categories.url` → `products.category_url`

**2. Categories ↔ Categories (1-N - Self-referential):**
- 1 category cha có nhiều category con
- 1 category con có 1 category cha (hoặc NULL nếu là root)
- **Relationship:** `categories.parent_url` → `categories.url`

### 5.1.4. Lý do Chuẩn hóa 3NF

**1NF (First Normal Form):**
- ✅ Mỗi cell chỉ chứa 1 giá trị atomic
- ✅ Không có duplicate rows
- ✅ Các columns có tên unique

**2NF (Second Normal Form):**
- ✅ Tất cả non-key attributes phụ thuộc hoàn toàn vào primary key
- ✅ Không có partial dependencies

**3NF (Third Normal Form):**
- ✅ Tất cả non-key attributes chỉ phụ thuộc vào primary key
- ✅ Không có transitive dependencies

**Ví dụ trong products table:**
- `estimated_revenue` phụ thuộc vào `sales_count` và `price` (computed field)
- Tuy nhiên, vì đây là derived field, không vi phạm 3NF (có thể tính lại từ base fields)

**Denormalization:**
- `products` table chứa cả seller fields và brand (thay vì tách thành sellers và brands tables)
- **Lý do:** Đơn giản hóa schema, giảm joins khi query
- **Trade-off:** Có thể có data redundancy nếu cùng 1 seller/brand xuất hiện nhiều lần

## 5.2. Warehouse (Star Schema)

### 5.2.1. Fact Table

**Fact Table: `products` (denormalized)**

**Grain (Granularity):**
- **Grain:** 1 row = 1 product snapshot tại 1 thời điểm
- **Có thể mở rộng:** Nếu cần lưu lịch sử thay đổi → tạo `products_history` table với grain: 1 row = 1 product tại 1 thời điểm

**Measures (Facts):**
- `sales_count` (Integer): Số lượng đã bán
- `estimated_revenue` (Decimal): Doanh thu ước tính = sales_count × price
- `price` (Decimal): Giá hiện tại
- `original_price` (Decimal): Giá gốc
- `discount_percent` (Integer): Phần trăm giảm giá
- `popularity_score` (Decimal): Điểm độ phổ biến (0-100)
- `value_score` (Decimal): Điểm giá trị
- `review_count` (Integer): Số lượng review

**Dimensions (Foreign Keys to Dimensions):**
- `category_url` → Dim_Category
- `category_id` → Dim_Category
- `seller_id` → Dim_Seller (nếu có)
- `brand` → Dim_Brand (nếu có)
- `crawled_at` → Dim_Time (nếu cần)

### 5.2.2. Dimension Tables

**1. Dim_Category (Conceptual, hiện tại trong `categories` table)**

| Attribute | Type | Mô tả |
|-----------|------|-------|
| category_id (PK) | VARCHAR(255) | Surrogate/Natural key |
| category_name | VARCHAR(500) | Tên danh mục |
| category_url | TEXT | URL danh mục |
| parent_category_id | VARCHAR(255) | ID danh mục cha |
| category_level | INTEGER | Cấp độ danh mục |
| category_path | TEXT | Breadcrumb path |

**Surrogate Key:** Có thể tạo `category_key` (SERIAL) nếu cần

**2. Dim_Seller (Conceptual, hiện tại denormalized trong `products`)**

| Attribute | Type | Mô tả |
|-----------|------|-------|
| seller_id (PK) | VARCHAR(255) | Natural key |
| seller_name | VARCHAR(500) | Tên người bán |
| seller_is_official | BOOLEAN | Có phải seller chính thức |

**3. Dim_Brand (Conceptual, hiện tại denormalized trong `products`)**

| Attribute | Type | Mô tả |
|-----------|------|-------|
| brand_id (PK) | VARCHAR(255) | Natural key (brand name) |
| brand_name | VARCHAR(255) | Tên thương hiệu |

**4. Dim_Time (Nếu cần phân tích theo thời gian)**

| Attribute | Type | Mô tả |
|-----------|------|-------|
| date_key (PK) | INTEGER | Date key (YYYYMMDD) |
| date | DATE | Ngày |
| day | INTEGER | Ngày trong tháng |
| month | INTEGER | Tháng |
| year | INTEGER | Năm |
| quarter | INTEGER | Quý |
| week | INTEGER | Tuần |
| day_name | VARCHAR(10) | Tên ngày (Monday, ...) |

### 5.2.3. Cách Chọn Grain

**Grain của Fact Table:**

**Option 1: Product Snapshot (Hiện tại)**
- 1 row = 1 product tại 1 thời điểm
- **Pros:** Đơn giản, dễ query
- **Cons:** Không lưu lịch sử thay đổi

**Option 2: Product History**
- 1 row = 1 product tại 1 thời điểm (snapshot)
- **Pros:** Có thể phân tích xu hướng thay đổi giá, sales
- **Cons:** Tăng kích thước database đáng kể

**Chọn Option 1** vì:
- Dữ liệu crawl mới nhất là quan trọng nhất
- Có thể so sánh với lần crawl trước bằng cách lưu backup JSON
- Đơn giản hóa schema và queries

### 5.2.4. Lý do Dùng Kimball

**Kimball Methodology:**
- **Star Schema:** Denormalized để tối ưu query performance
- **Slowly Changing Dimensions (SCD):** Type 1 (overwrite) hoặc Type 2 (historical) nếu cần
- **Conformed Dimensions:** Đảm bảo dimensions được sử dụng nhất quán
- **Business Process Focus:** Focus vào business questions (sản phẩm nào bán chạy? giá cả như thế nào?)

**Lý do chọn Kimball thay vì Inmon:**
- **Query Performance:** Star schema có ít joins hơn, query nhanh hơn
- **Business User Friendly:** Dễ hiểu và sử dụng cho BI tools
- **Iterative Development:** Có thể build từng data mart một

## 5.3. Data Mart cho DA

### 5.3.1. Mart theo Mục tiêu KPI

**1. Products Performance Mart**

**Mục tiêu:** Đánh giá hiệu suất sản phẩm

**Metrics:**
- Total products
- Total sales count
- Total revenue
- Average rating
- Average popularity score

**Dimensions:**
- Category
- Brand
- Price Category
- Seller (Official vs Non-official)

**SQL View:**
```sql
CREATE VIEW products_performance_mart AS
SELECT 
    category_url,
    price_category,
    brand,
    seller_is_official,
    COUNT(*) as product_count,
    SUM(sales_count) as total_sales,
    SUM(estimated_revenue) as total_revenue,
    AVG(rating_average) as avg_rating,
    AVG(popularity_score) as avg_popularity,
    AVG(value_score) as avg_value
FROM products
WHERE sales_count > 0
GROUP BY category_url, price_category, brand, seller_is_official;
```

**2. Sales Mart**

**Mục tiêu:** Phân tích doanh số

**Metrics:**
- Total sales count
- Total revenue
- Average price
- Discount percent

**Dimensions:**
- Category
- Time (crawled_at)

**SQL View:**
```sql
CREATE VIEW sales_mart AS
SELECT 
    category_url,
    DATE(crawled_at) as sale_date,
    COUNT(*) as product_count,
    SUM(sales_count) as total_sales,
    SUM(estimated_revenue) as total_revenue,
    AVG(price) as avg_price,
    AVG(discount_percent) as avg_discount
FROM products
WHERE sales_count > 0
GROUP BY category_url, DATE(crawled_at);
```

**3. Pricing Mart**

**Mục tiêu:** Phân tích giá cả

**Metrics:**
- Min price
- Max price
- Average price
- Average discount

**Dimensions:**
- Category
- Price Category
- Brand

**SQL View:**
```sql
CREATE VIEW pricing_mart AS
SELECT 
    category_url,
    price_category,
    brand,
    COUNT(*) as product_count,
    MIN(price) as min_price,
    MAX(price) as max_price,
    AVG(price) as avg_price,
    AVG(discount_percent) as avg_discount,
    AVG(price_savings) as avg_savings
FROM products
WHERE price IS NOT NULL
GROUP BY category_url, price_category, brand;
```

### 5.3.2. Chỉ số Phân tích

**KPI chính:**
1. **Total Revenue:** Tổng doanh thu ước tính
2. **Average Sales Count:** Trung bình số lượng bán
3. **Top Products:** Top 10 sản phẩm bán chạy nhất
4. **Category Performance:** Doanh thu theo category
5. **Price Analysis:** Phân bố giá theo category/brand

**Metrics nâng cao:**
1. **Popularity Score:** Điểm độ phổ biến (0-100)
2. **Value Score:** Điểm giá trị (rating/price)
3. **Discount Impact:** Mối quan hệ giữa discount và sales

### 5.3.3. Logic Tính toán

**Popularity Score:**
```
popularity_score = (sales_count / max_sales) * 50 + 
                   (rating_avg / 5) * 30 + 
                   (review_count / max_reviews) * 20
```

**Value Score:**
```
value_score = rating_avg / (price / 1,000,000)
```

**Estimated Revenue:**
```
estimated_revenue = sales_count * price
```

**Price Category:**
```
if price < 500,000: "budget"
elif price < 2,000,000: "mid-range"
elif price < 10,000,000: "premium"
else: "luxury"
```

---

# 6. BUSINESS LOGIC

Phần này trình bày chi tiết các quy tắc nghiệp vụ (business rules) được áp dụng trong hệ thống, cách dữ liệu được tính toán và xử lý theo từng trường hợp cụ thể.

## 6.1. Các Quy tắc Nghiệp vụ

### 6.1.1. Validation Rules

**1. Product ID Validation:**
- Product ID phải là số (chỉ chứa chữ số 0-9)
- Product ID không được rỗng
- Product ID phải unique trong database

**2. Price Validation:**
- Giá hiện tại (`price`) phải >= 0
- Giá gốc (`original_price`) phải >= 0
- Giá hiện tại không được lớn hơn giá gốc (`price <= original_price`)
- Nếu có discount: `discount_percent` phải trong khoảng 0-100

**3. Rating Validation:**
- Rating trung bình (`rating_average`) phải trong khoảng 0-5
- Số lượng review (`review_count`) phải >= 0
- Nếu có rating nhưng không có review_count → vẫn hợp lệ (có thể là rating trung bình từ ít review)

**4. Sales Count Validation:**
- Sales count phải >= 0
- Sales count có thể NULL nếu chưa có dữ liệu

**5. URL Validation:**
- URL phải bắt đầu bằng `http://` hoặc `https://`
- URL phải hợp lệ (có thể parse được)
- Category URL phải thuộc domain `tiki.vn`

### 6.1.2. Business Rules cho Computed Fields

**1. Estimated Revenue:**
```
IF sales_count IS NOT NULL AND price IS NOT NULL:
    estimated_revenue = sales_count * price
ELSE:
    estimated_revenue = NULL
```

**Lý do:** Chỉ tính doanh thu khi có đủ thông tin về số lượng bán và giá.

**2. Price Savings:**
```
IF original_price IS NOT NULL AND price IS NOT NULL AND original_price > price:
    price_savings = original_price - price
ELSE:
    price_savings = NULL
```

**Lý do:** Chỉ tính tiết kiệm khi có giá gốc và giá hiện tại, và giá hiện tại thấp hơn giá gốc.

**3. Discount Percent:**
```
IF original_price IS NOT NULL AND price IS NOT NULL AND original_price > 0:
    discount_percent = ((original_price - price) / original_price) * 100
    discount_percent = ROUND(discount_percent, 2)
ELSE IF discount_percent IS PROVIDED:
    discount_percent = discount_percent (giữ nguyên)
ELSE:
    discount_percent = NULL
```

**Lý do:** Tính lại discount_percent từ giá nếu có thể, hoặc giữ nguyên giá trị từ nguồn.

**4. Price Category:**
```
IF price IS NULL:
    price_category = NULL
ELIF price < 500,000:
    price_category = "budget"
ELIF price < 2,000,000:
    price_category = "mid-range"
ELIF price < 10,000,000:
    price_category = "premium"
ELSE:
    price_category = "luxury"
```

**Lý do:** Phân loại sản phẩm theo giá để phân tích thị trường:
- **Budget:** Sản phẩm giá rẻ (< 500k)
- **Mid-range:** Sản phẩm tầm trung (500k - 2M)
- **Premium:** Sản phẩm cao cấp (2M - 10M)
- **Luxury:** Sản phẩm xa xỉ (> 10M)

**5. Popularity Score (0-100):**
```
popularity_score = 0

IF sales_count IS NOT NULL:
    sales_score = MIN((sales_count / max_sales) * 50, 50)
    popularity_score += sales_score
    # max_sales = 100,000 (normalization constant)

IF rating_average IS NOT NULL:
    rating_score = (rating_average / 5) * 30
    popularity_score += rating_score

IF review_count IS NOT NULL:
    review_score = MIN((review_count / max_reviews) * 20, 20)
    popularity_score += review_score
    # max_reviews = 10,000 (normalization constant)

popularity_score = ROUND(popularity_score, 2)

IF popularity_score == 0:
    popularity_score = NULL
```

**Lý do:** Tính điểm độ phổ biến dựa trên:
- **Sales count (50%):** Sản phẩm bán chạy hơn → điểm cao hơn
- **Rating (30%):** Sản phẩm được đánh giá tốt hơn → điểm cao hơn
- **Review count (20%):** Sản phẩm có nhiều review hơn → điểm cao hơn

**6. Value Score:**
```
IF rating_average IS NOT NULL AND price IS NOT NULL AND price > 0:
    price_million = price / 1,000,000
    value_score = rating_average / price_million
    value_score = ROUND(value_score, 2)
ELSE:
    value_score = NULL
```

**Lý do:** Tính điểm giá trị: sản phẩm có rating cao và giá thấp sẽ có value_score cao hơn (giá trị tốt hơn).

**7. Discount Amount:**
```
IF price_savings IS NOT NULL:
    discount_amount = price_savings
ELSE IF original_price IS NOT NULL AND price IS NOT NULL AND original_price > price:
    discount_amount = original_price - price
ELSE:
    discount_amount = NULL
```

**Lý do:** Tính số tiền giảm (giống price_savings, nhưng đặt tên rõ ràng hơn).

**8. Sales Velocity:**
```
IF sales_count IS NOT NULL:
    sales_velocity = sales_count
ELSE:
    sales_velocity = NULL
```

**Lý do:** Tốc độ bán (hiện tại chỉ là sales_count, có thể tính chi tiết hơn nếu có dữ liệu theo thời gian).

## 6.2. Luồng Xử lý Chi tiết theo Từng Trường hợp

### 6.2.1. Trường hợp 1: Product Mới (Chưa có trong Database)

**Flow:**
1. **Crawl product từ Tiki.vn**
2. **Validate data:**
   - Kiểm tra required fields (product_id, name, url)
   - Validate format (product_id là số, URL hợp lệ)
3. **Transform:**
   - Normalize text, numbers, dates
   - Flatten nested structures
   - Compute derived fields
4. **Load:**
   - INSERT vào database
   - Lưu vào JSON backup file
   - Tạo asset `tiki://products/final`

**Output:**
- Product record mới trong database
- Log: "Product {product_id} inserted successfully"

### 6.2.2. Trường hợp 2: Product Đã Tồn tại (Update)

**Flow:**
1. **Crawl product từ Tiki.vn** (như trường hợp 1)
2. **Validate và Transform** (như trường hợp 1)
3. **Load với UPSERT:**
   - Kiểm tra `product_id` đã tồn tại
   - Nếu có → UPDATE tất cả fields
   - Set `updated_at = CURRENT_TIMESTAMP`
   - Nếu không → INSERT (như trường hợp 1)

**Output:**
- Product record được update trong database
- Log: "Product {product_id} updated successfully"

### 6.2.3. Trường hợp 3: Product Thiếu Dữ liệu

**Flow:**
1. **Crawl product cơ bản** (chỉ có product_id, name, url)
2. **Crawl product detail** (bổ sung price, rating, description, etc.)
3. **Merge dữ liệu:**
   - Merge product cơ bản với product detail
   - Nếu detail crawl fail → vẫn lưu product cơ bản
4. **Transform và Load** (như trường hợp 1)

**Output:**
- Product record với dữ liệu cơ bản (nếu detail fail)
- Hoặc product record đầy đủ (nếu detail success)
- Log: "Product {product_id} saved with partial data" hoặc "Product {product_id} saved with full data"

### 6.2.4. Trường hợp 4: Product Duplicate

**Flow:**
1. **Crawl product từ Tiki.vn**
2. **Transform và Validate**
3. **Kiểm tra duplicate:**
   - Query database: `SELECT * FROM products WHERE product_id = ?`
   - Nếu có → SKIP (hoặc UPDATE nếu muốn)
4. **Nếu không duplicate:**
   - INSERT vào database

**Output:**
- Product record không bị duplicate
- Log: "Duplicate product {product_id} skipped" hoặc "Product {product_id} inserted"

### 6.2.5. Trường hợp 5: Product Validation Fail

**Flow:**
1. **Crawl product từ Tiki.vn**
2. **Transform**
3. **Validate:**
   - Nếu FAIL → Log error và SKIP
   - Nếu PASS → Continue
4. **Load** (chỉ khi validation pass)

**Output:**
- Product record không được insert
- Log error: "Product {product_id} validation failed: {error_message}"

## 6.3. Cách Dữ liệu được Tính toán (Ví dụ KPI, Tỷ lệ, Tăng trưởng)

### 6.3.1. KPI Calculation

**1. Total Revenue (Tổng Doanh thu):**
```sql
SELECT SUM(estimated_revenue) as total_revenue
FROM products
WHERE estimated_revenue IS NOT NULL;
```

**2. Average Sales Count (Trung bình Số lượng Bán):**
```sql
SELECT AVG(sales_count) as avg_sales_count
FROM products
WHERE sales_count IS NOT NULL;
```

**3. Average Rating (Trung bình Đánh giá):**
```sql
SELECT AVG(rating_average) as avg_rating
FROM products
WHERE rating_average IS NOT NULL;
```

**4. Top 10 Products (Top 10 Sản phẩm Bán chạy):**
```sql
SELECT product_id, name, sales_count, estimated_revenue
FROM products
WHERE sales_count IS NOT NULL
ORDER BY sales_count DESC
LIMIT 10;
```

### 6.3.2. Tỷ lệ và Phân bố

**1. Product Distribution by Price Category:**
```sql
SELECT 
    price_category,
    COUNT(*) as product_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
FROM products
WHERE price_category IS NOT NULL
GROUP BY price_category
ORDER BY product_count DESC;
```

**2. Product Distribution by Category:**
```sql
SELECT 
    category_url,
    COUNT(*) as product_count,
    SUM(estimated_revenue) as total_revenue,
    AVG(price) as avg_price
FROM products
WHERE category_url IS NOT NULL
GROUP BY category_url
ORDER BY total_revenue DESC;
```

**3. Discount Distribution:**
```sql
SELECT 
    CASE 
        WHEN discount_percent = 0 THEN 'No discount'
        WHEN discount_percent < 10 THEN '0-10%'
        WHEN discount_percent < 20 THEN '10-20%'
        WHEN discount_percent < 30 THEN '20-30%'
        ELSE '>30%'
    END as discount_range,
    COUNT(*) as product_count
FROM products
WHERE discount_percent IS NOT NULL
GROUP BY discount_range
ORDER BY discount_range;
```

### 6.3.3. Tăng trưởng (Growth)

**Lưu ý:** Trong dự án hiện tại, chỉ lưu snapshot mới nhất, không lưu lịch sử. Để tính tăng trưởng, cần:

**Option 1: So sánh với Backup JSON**
- Lưu backup JSON mỗi lần crawl
- So sánh `products_final_2024-01-01.json` vs `products_final_2024-01-02.json`

**Option 2: Tạo History Table**
```sql
CREATE TABLE products_history (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(255),
    sales_count INTEGER,
    price DECIMAL(12, 2),
    crawled_at TIMESTAMP,
    snapshot_date DATE
);
```

**Ví dụ Growth Calculation:**
```sql
WITH current_snapshot AS (
    SELECT product_id, sales_count, price, crawled_at
    FROM products
    WHERE DATE(crawled_at) = CURRENT_DATE
),
previous_snapshot AS (
    SELECT product_id, sales_count, price, crawled_at
    FROM products_history
    WHERE snapshot_date = CURRENT_DATE - INTERVAL '1 day'
)
SELECT 
    c.product_id,
    c.sales_count - COALESCE(p.sales_count, 0) as sales_growth,
    c.price - COALESCE(p.price, 0) as price_change
FROM current_snapshot c
LEFT JOIN previous_snapshot p ON c.product_id = p.product_id;
```

## 6.4. Ví dụ Dữ liệu Trước và Sau Khi Xử lý

### 6.4.1. Trước Khi Xử lý (Raw Data)

```json
{
  "product_id": "123456789",
  "name": "  iPhone 15 Pro Max  ",
  "url": "https://tiki.vn/iphone-15-pro-max-p123456789.html",
  "image_url": "https://...",
  "category_url": "https://tiki.vn/dien-thoai/c1789",
  "sales_count": "2,000",
  "price": {
    "current_price": "28,990,000",
    "original_price": "32,990,000",
    "discount_percent": "12.13"
  },
  "rating": {
    "average": "4.5",
    "total_reviews": "150"
  },
  "brand": "Thương hiệu: Apple",
  "seller": {
    "name": "Tiki Trading",
    "seller_id": "123",
    "is_official": "true"
  }
}
```

**Vấn đề:**
- Text có whitespace thừa
- Numbers là string với dấu phẩy
- Nested structures
- Brand có prefix không cần thiết
- Thiếu computed fields

### 6.4.2. Sau Khi Xử lý (Transformed Data)

```json
{
  "product_id": "123456789",
  "name": "iPhone 15 Pro Max",
  "url": "https://tiki.vn/iphone-15-pro-max-p123456789.html",
  "image_url": "https://...",
  "category_url": "https://tiki.vn/dien-thoai/c1789",
  "category_id": "c1789",
  "sales_count": 2000,
  "price": 28990000,
  "original_price": 32990000,
  "discount_percent": 12,
  "rating_average": 4.5,
  "review_count": 150,
  "brand": "Apple",
  "seller_name": "Tiki Trading",
  "seller_id": "123",
  "seller_is_official": true,
  "estimated_revenue": 57980000000,
  "price_savings": 4000000,
  "discount_amount": 4000000,
  "price_category": "luxury",
  "popularity_score": 85.5,
  "value_score": 0.155,
  "sales_velocity": 2000,
  "crawled_at": "2024-01-01T12:00:00",
  "updated_at": "2024-01-01T12:00:00"
}
```

**Cải thiện:**
- ✅ Text đã được normalize (trim whitespace)
- ✅ Numbers đã được parse thành int/float
- ✅ Nested structures đã được flatten
- ✅ Brand đã được normalize (loại bỏ prefix)
- ✅ Có computed fields (estimated_revenue, popularity_score, value_score, etc.)
- ✅ Có category_id (extract từ category_url)
- ✅ Có timestamps (crawled_at, updated_at)

**Giải thích Computed Fields:**

- **estimated_revenue = 57,980,000,000 VND**
  - = 2000 (sales_count) × 28,990,000 (price)

- **price_savings = 4,000,000 VND**
  - = 32,990,000 (original_price) - 28,990,000 (price)

- **price_category = "luxury"**
  - Vì price = 28,990,000 > 10,000,000

- **popularity_score = 85.5**
  - Sales score: (2000 / 100000) * 50 = 1.0
  - Rating score: (4.5 / 5) * 30 = 27.0
  - Review score: (150 / 10000) * 20 = 0.3
  - Total: 1.0 + 27.0 + 0.3 = 28.3
  - *(Lưu ý: Ví dụ này có vẻ không đúng, vì sales_count 2000 nên sales_score phải cao hơn. Công thức thực tế có thể khác)*

- **value_score = 0.155**
  - = 4.5 (rating_average) / (28,990,000 / 1,000,000)
  - = 4.5 / 28.99 ≈ 0.155

---

# 7. PHÂN TÍCH DỮ LIỆU (DA SECTION)

Phần này mô tả các phương pháp phân tích dữ liệu, các câu hỏi nghiệp vụ (business questions), KPI cần theo dõi, và insights có thể rút ra từ dữ liệu.

## 7.1. Các Câu hỏi Phân tích (Business Questions)

### 7.1.1. Câu hỏi về Sản phẩm

**1. Sản phẩm nào bán chạy nhất?**
- **Mục đích:** Xác định sản phẩm phổ biến để hiểu xu hướng thị trường
- **Metrics:** sales_count, estimated_revenue
- **Analysis:**
  ```sql
  SELECT name, sales_count, estimated_revenue, popularity_score
  FROM products
  ORDER BY sales_count DESC
  LIMIT 20;
  ```

**2. Sản phẩm nào có giá trị tốt nhất?**
- **Mục đích:** Tìm sản phẩm có rating cao và giá hợp lý
- **Metrics:** value_score, rating_average, price
- **Analysis:**
  ```sql
  SELECT name, price, rating_average, value_score
  FROM products
  WHERE value_score IS NOT NULL
  ORDER BY value_score DESC
  LIMIT 20;
  ```

**3. Sản phẩm nào có độ phổ biến cao nhất?**
- **Mục đích:** Xác định sản phẩm được nhiều người quan tâm
- **Metrics:** popularity_score
- **Analysis:**
  ```sql
  SELECT name, popularity_score, sales_count, rating_average
  FROM products
  WHERE popularity_score IS NOT NULL
  ORDER BY popularity_score DESC
  LIMIT 20;
  ```

### 7.1.2. Câu hỏi về Danh mục

**4. Danh mục nào có doanh thu cao nhất?**
- **Mục đích:** Xác định danh mục mang lại giá trị kinh doanh cao
- **Metrics:** total_revenue by category
- **Analysis:**
  ```sql
  SELECT 
      category_url,
      COUNT(*) as product_count,
      SUM(estimated_revenue) as total_revenue,
      AVG(price) as avg_price
  FROM products
  WHERE category_url IS NOT NULL
  GROUP BY category_url
  ORDER BY total_revenue DESC
  LIMIT 10;
  ```

**5. Danh mục nào có giá trung bình cao nhất/thấp nhất?**
- **Mục đích:** Hiểu phân khúc giá theo danh mục
- **Metrics:** avg_price by category
- **Analysis:**
  ```sql
  SELECT 
      category_url,
      AVG(price) as avg_price,
      MIN(price) as min_price,
      MAX(price) as max_price
  FROM products
  WHERE price IS NOT NULL
  GROUP BY category_url
  ORDER BY avg_price DESC;
  ```

### 7.1.3. Câu hỏi về Giá cả

**6. Phân bố giá theo category như thế nào?**
- **Mục đích:** Hiểu cấu trúc giá trong từng danh mục
- **Metrics:** price distribution
- **Analysis:**
  ```sql
  SELECT 
      category_url,
      price_category,
      COUNT(*) as product_count,
      AVG(price) as avg_price
  FROM products
  WHERE price IS NOT NULL
  GROUP BY category_url, price_category
  ORDER BY category_url, avg_price;
  ```

**7. Tỷ lệ giảm giá trung bình theo category?**
- **Mục đích:** Hiểu chiến lược giảm giá của từng danh mục
- **Metrics:** avg_discount_percent by category
- **Analysis:**
  ```sql
  SELECT 
      category_url,
      AVG(discount_percent) as avg_discount,
      COUNT(*) as discounted_products
  FROM products
  WHERE discount_percent IS NOT NULL AND discount_percent > 0
  GROUP BY category_url
  ORDER BY avg_discount DESC;
  ```

### 7.1.4. Câu hỏi về Brand và Seller

**8. Brand nào có nhiều sản phẩm nhất?**
- **Mục đích:** Xác định brand chiếm thị phần lớn
- **Metrics:** product_count by brand
- **Analysis:**
  ```sql
  SELECT 
      brand,
      COUNT(*) as product_count,
      AVG(price) as avg_price,
      SUM(estimated_revenue) as total_revenue
  FROM products
  WHERE brand IS NOT NULL
  GROUP BY brand
  ORDER BY product_count DESC
  LIMIT 20;
  ```

**9. Sản phẩm từ seller chính thức có doanh thu cao hơn không?**
- **Mục đích:** So sánh hiệu suất giữa seller chính thức và không chính thức
- **Metrics:** revenue by seller_is_official
- **Analysis:**
  ```sql
  SELECT 
      seller_is_official,
      COUNT(*) as product_count,
      SUM(estimated_revenue) as total_revenue,
      AVG(price) as avg_price,
      AVG(rating_average) as avg_rating
  FROM products
  WHERE seller_is_official IS NOT NULL
  GROUP BY seller_is_official;
  ```

## 7.2. Phương pháp Phân tích

### 7.2.1. Descriptive Analytics (Mô tả)

**Mục đích:** Mô tả dữ liệu hiện tại

**Methods:**
- **Summary Statistics:** Mean, median, mode, std deviation
- **Frequency Distribution:** Count, percentage
- **Visualization:** Bar charts, pie charts, histograms

**Ví dụ:**
```sql
-- Summary statistics
SELECT 
    COUNT(*) as total_products,
    AVG(price) as avg_price,
    MIN(price) as min_price,
    MAX(price) as max_price,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) as median_price
FROM products
WHERE price IS NOT NULL;
```

### 7.2.2. Diagnostic Analytics (Chẩn đoán)

**Mục đích:** Hiểu tại sao một sự kiện xảy ra

**Methods:**
- **Correlation Analysis:** Tìm mối quan hệ giữa các biến
- **Segmentation:** Phân nhóm dữ liệu
- **Drill-down:** Đi sâu vào chi tiết

**Ví dụ:**
```sql
-- Correlation giữa discount và sales
SELECT 
    CASE 
        WHEN discount_percent = 0 THEN 'No discount'
        WHEN discount_percent < 10 THEN '0-10%'
        WHEN discount_percent < 20 THEN '10-20%'
        ELSE '>20%'
    END as discount_range,
    AVG(sales_count) as avg_sales,
    AVG(estimated_revenue) as avg_revenue
FROM products
WHERE discount_percent IS NOT NULL AND sales_count IS NOT NULL
GROUP BY discount_range
ORDER BY discount_range;
```

### 7.2.3. Predictive Analytics (Dự đoán)

**Mục đích:** Dự đoán xu hướng tương lai (nếu có dữ liệu lịch sử)

**Methods:**
- **Time Series Analysis:** Phân tích xu hướng theo thời gian
- **Regression:** Dự đoán giá trị dựa trên các biến
- **Machine Learning:** Model dự đoán (nếu có)

**Ví dụ (conceptual):**
```sql
-- Time series analysis (nếu có products_history table)
SELECT 
    DATE(snapshot_date) as date,
    AVG(price) as avg_price,
    SUM(sales_count) as total_sales
FROM products_history
WHERE snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY DATE(snapshot_date)
ORDER BY date;
```

## 7.3. KPI Chính Cần Theo dõi

### 7.3.1. Product KPIs

**1. Total Products (Tổng số sản phẩm)**
- **Metric:** COUNT(*) FROM products
- **Frequency:** Daily
- **Target:** Tăng trưởng hàng tháng

**2. Top Products (Top sản phẩm bán chạy)**
- **Metric:** Top 10 products by sales_count
- **Frequency:** Daily
- **Target:** Maintain top products

**3. Average Price (Giá trung bình)**
- **Metric:** AVG(price) FROM products
- **Frequency:** Daily
- **Target:** Theo dõi xu hướng giá

**4. Average Rating (Đánh giá trung bình)**
- **Metric:** AVG(rating_average) FROM products
- **Frequency:** Daily
- **Target:** >= 4.0 (nếu có)

### 7.3.2. Revenue KPIs

**5. Total Revenue (Tổng doanh thu)**
- **Metric:** SUM(estimated_revenue) FROM products
- **Frequency:** Daily
- **Target:** Tăng trưởng hàng tháng

**6. Revenue by Category (Doanh thu theo danh mục)**
- **Metric:** SUM(estimated_revenue) GROUP BY category_url
- **Frequency:** Weekly
- **Target:** Identify top categories

**7. Average Revenue per Product (Doanh thu trung bình mỗi sản phẩm)**
- **Metric:** AVG(estimated_revenue) FROM products
- **Frequency:** Daily
- **Target:** Increase over time

### 7.3.3. Pricing KPIs

**8. Average Discount (Giảm giá trung bình)**
- **Metric:** AVG(discount_percent) FROM products WHERE discount_percent > 0
- **Frequency:** Daily
- **Target:** Monitor pricing strategy

**9. Price Range Distribution (Phân bố giá)**
- **Metric:** COUNT(*) GROUP BY price_category
- **Frequency:** Weekly
- **Target:** Understand market segments

### 7.3.4. Quality KPIs

**10. Data Completeness (Độ đầy đủ dữ liệu)**
- **Metric:** % products với đầy đủ required fields
- **Frequency:** Daily
- **Target:** >= 95%

**11. Average Popularity Score (Điểm độ phổ biến trung bình)**
- **Metric:** AVG(popularity_score) FROM products
- **Frequency:** Daily
- **Target:** Monitor product popularity

## 7.4. Các Chỉ số Nâng cao (Growth, Retention, Churn)

### 7.4.1. Growth Metrics

**1. Sales Growth (Tăng trưởng doanh số)**
- **Metric:** (Current sales_count - Previous sales_count) / Previous sales_count * 100
- **Note:** Cần dữ liệu lịch sử để tính

**2. Product Growth (Tăng trưởng số lượng sản phẩm)**
- **Metric:** (Current product_count - Previous product_count) / Previous product_count * 100
- **Frequency:** Monthly

**3. Revenue Growth (Tăng trưởng doanh thu)**
- **Metric:** (Current revenue - Previous revenue) / Previous revenue * 100
- **Frequency:** Monthly

### 7.4.2. Retention Metrics (Nếu có dữ liệu lịch sử)

**1. Product Retention Rate (Tỷ lệ giữ chân sản phẩm)**
- **Metric:** % products xuất hiện trong 2 lần crawl liên tiếp
- **Note:** Cần lưu lịch sử crawl

**2. Category Retention Rate (Tỷ lệ giữ chân danh mục)**
- **Metric:** % categories có products trong 2 lần crawl liên tiếp

### 7.4.3. Churn Metrics (Nếu có dữ liệu lịch sử)

**1. Product Churn Rate (Tỷ lệ rời bỏ sản phẩm)**
- **Metric:** % products biến mất giữa 2 lần crawl
- **Note:** Cần so sánh 2 snapshot

**2. Price Change Rate (Tỷ lệ thay đổi giá)**
- **Metric:** % products có giá thay đổi giữa 2 lần crawl

## 7.5. Insight Rút ra từ Dữ liệu

### 7.5.1. Product Insights

**Insight 1: Sản phẩm giá rẻ bán chạy hơn**
- **Evidence:** Products trong price_category "budget" có avg_sales_count cao hơn "luxury"
- **Action:** Tập trung vào sản phẩm giá rẻ để tăng doanh số

**Insight 2: Rating cao không đảm bảo bán chạy**
- **Evidence:** Một số products có rating 5.0 nhưng sales_count thấp
- **Action:** Cần phân tích thêm (có thể do giá cao hoặc ít review)

**Insight 3: Discount không phải lúc nào cũng tăng sales**
- **Evidence:** Một số products có discount cao nhưng sales_count thấp
- **Action:** Cần phân tích correlation giữa discount và sales

### 7.5.2. Category Insights

**Insight 4: Danh mục điện tử có doanh thu cao nhất**
- **Evidence:** Category "Điện tử" có total_revenue cao nhất
- **Action:** Tập trung vào danh mục này để tăng doanh thu

**Insight 5: Danh mục thời trang có giá trung bình cao**
- **Evidence:** Category "Thời trang" có avg_price cao hơn các category khác
- **Action:** Cần phân tích thêm về margin và profit

### 7.5.3. Market Insights

**Insight 6: Thị trường tập trung vào sản phẩm tầm trung**
- **Evidence:** Phần lớn products thuộc price_category "mid-range"
- **Action:** Phân khúc thị trường chính là mid-range

**Insight 7: Seller chính thức có rating cao hơn**
- **Evidence:** Products từ seller_is_official = true có avg_rating cao hơn
- **Action:** Khuyến khích mua từ seller chính thức

## 7.6. Framework Phân tích

### 7.6.1. Descriptive Analytics

**Mục đích:** Mô tả dữ liệu hiện tại

**Methods:**
- Summary statistics
- Frequency distribution
- Visualization

**Tools:**
- SQL queries
- Excel/Power BI
- Python/Pandas

### 7.6.2. Diagnostic Analytics

**Mục đích:** Hiểu tại sao một sự kiện xảy ra

**Methods:**
- Correlation analysis
- Segmentation
- Drill-down

**Tools:**
- SQL với GROUP BY, JOIN
- Python với pandas, matplotlib
- Power BI với drill-down

### 7.6.3. Predictive Analytics (Future)

**Mục đích:** Dự đoán xu hướng tương lai

**Methods:**
- Time series analysis
- Regression
- Machine learning

**Tools:**
- Python với scikit-learn, statsmodels
- R với forecast package
- Azure ML / AWS SageMaker

---

# 8. DASHBOARD DESIGN

## 8.1. Mục tiêu Dashboard

Dashboard được thiết kế để cung cấp cái nhìn tổng quan và chi tiết về dữ liệu sản phẩm Tiki, hỗ trợ các quyết định kinh doanh dựa trên dữ liệu.

**Mục tiêu chính:**
- **Theo dõi KPI:** Hiển thị các chỉ số quan trọng (doanh thu, số lượng sản phẩm, rating trung bình)
- **Phân tích xu hướng:** Xác định sản phẩm bán chạy, danh mục phổ biến
- **So sánh và đối chiếu:** So sánh giá cả, đánh giá giữa các sản phẩm/danh mục
- **Hỗ trợ quyết định:** Cung cấp insights để đưa ra quyết định kinh doanh

## 8.2. Đối tượng Sử dụng

**1. Quản lý Kinh doanh:**
- Xem dashboard tổng quan hàng ngày
- Theo dõi xu hướng và KPI chính
- Nhận báo cáo tự động

**2. Nhà Phân tích Kinh doanh (Business Analyst):**
- Phân tích chi tiết dữ liệu
- Tạo báo cáo tùy chỉnh
- Drill-down vào từng sản phẩm/danh mục

**3. Nhà Quản lý Sản phẩm:**
- Theo dõi hiệu suất sản phẩm
- Phân tích giá cả và đối thủ
- Xác định cơ hội cải thiện

## 8.3. Các Trang Dashboard

### 8.3.1. Overview Dashboard (Trang Tổng quan)

**Mục đích:** Cung cấp cái nhìn tổng quan về toàn bộ hệ thống

**Components:**
- **KPI Cards (4 cards):**
  - Tổng số sản phẩm: `COUNT(*) FROM products`
  - Tổng doanh thu ước tính: `SUM(estimated_revenue)`
  - Trung bình rating: `AVG(rating_average)`
  - Trung bình sales count: `AVG(sales_count)`

- **Top 10 Sản phẩm Bán chạy (Bar Chart):**
  - X-axis: Tên sản phẩm (truncated)
  - Y-axis: Sales count
  - Tooltip: Hiển thị đầy đủ tên, giá, rating

- **Phân bố Sản phẩm theo Category (Pie Chart):**
  - Hiển thị % sản phẩm mỗi danh mục
  - Click để drill-down vào category detail

- **Phân bố Giá theo Price Category (Bar Chart):**
  - Budget, Mid-range, Premium, Luxury
  - Số lượng sản phẩm mỗi phân khúc

- **Top 10 Danh mục theo Doanh thu (Horizontal Bar Chart):**
  - X-axis: Doanh thu (VND)
  - Y-axis: Category name

### 8.3.2. Detail Dashboard (Trang Chi tiết)

**Mục đích:** Phân tích chi tiết theo từng dimension

**Tabs:**

**Tab 1: Products Detail**
- **Bảng sản phẩm:** 
  - Columns: ID, Tên, Category, Giá, Sales Count, Revenue, Rating, Popularity Score
  - Sortable, filterable, searchable
  - Pagination (50 items/page)

- **Product Detail Panel:**
  - Click vào sản phẩm → hiển thị chi tiết:
    - Hình ảnh, mô tả, thông số kỹ thuật
    - Lịch sử giá (nếu có)
    - So sánh với sản phẩm tương tự

**Tab 2: Category Analysis**
- **Category Performance Table:**
  - Columns: Category, Số sản phẩm, Tổng doanh thu, Giá trung bình, Rating trung bình
- **Category Tree Visualization:**
  - Hierarchical tree view
  - Click để filter products

**Tab 3: Pricing Analysis**
- **Price Distribution (Histogram):**
  - Phân bố giá theo khoảng
- **Discount Analysis:**
  - Tỷ lệ sản phẩm có discount
  - Mối quan hệ giữa discount và sales

**Tab 4: Brand & Seller Analysis**
- **Top Brands Table:**
  - Số sản phẩm, doanh thu, rating trung bình
- **Seller Comparison:**
  - Official vs Non-official sellers
  - Performance metrics

### 8.3.3. Drill-down Dashboard (Trang Phân tích Sâu)

**Mục đích:** Phân tích sâu vào từng sản phẩm/danh mục cụ thể

**Features:**
- **Category Drill-down:**
  - Click category trong Overview → xem tất cả sản phẩm trong category
  - Filter và sort sản phẩm
  - Export danh sách

- **Product Drill-down:**
  - Click sản phẩm → xem chi tiết đầy đủ
  - So sánh với sản phẩm khác trong cùng category
  - Phân tích giá trị (value_score)

- **Time-based Analysis (nếu có dữ liệu lịch sử):**
  - Xu hướng giá theo thời gian
  - Xu hướng sales theo thời gian
  - Growth rate

## 8.4. Các Biểu đồ và Lý do Chọn

### 8.4.1. Bar Chart (Cột)

**Sử dụng cho:**
- Top 10 sản phẩm bán chạy
- Doanh thu theo category
- So sánh metrics giữa các nhóm

**Lý do:** Dễ so sánh giá trị giữa các items, phù hợp cho dữ liệu categorical

### 8.4.2. Pie Chart (Tròn)

**Sử dụng cho:**
- Phân bố sản phẩm theo category
- Phân bố theo price category

**Lý do:** Hiển thị tỷ lệ phần trăm rõ ràng, dễ hiểu cho người dùng không chuyên

### 8.4.3. Line Chart (Đường)

**Sử dụng cho:**
- Xu hướng giá theo thời gian (nếu có)
- Xu hướng sales theo thời gian

**Lý do:** Phù hợp cho dữ liệu time-series, hiển thị xu hướng tốt

### 8.4.4. Histogram (Biểu đồ Tần suất)

**Sử dụng cho:**
- Phân bố giá
- Phân bố rating

**Lý do:** Hiển thị distribution của dữ liệu số liên tục

### 8.4.5. Scatter Plot (Biểu đồ Phân tán)

**Sử dụng cho:**
- Mối quan hệ giữa price và sales
- Mối quan hệ giữa rating và sales

**Lý do:** Phát hiện correlation và outliers

### 8.4.6. Table (Bảng)

**Sử dụng cho:**
- Danh sách sản phẩm chi tiết
- Category performance
- Brand comparison

**Lý do:** Hiển thị nhiều thông tin chi tiết, có thể sort/filter/search

## 8.5. Mô tả Logic từng KPI

### 8.5.1. Total Products (Tổng số Sản phẩm)

**Công thức:**
```sql
SELECT COUNT(*) FROM products;
```

**Logic:** Đếm tổng số sản phẩm đã crawl và lưu trong database

**Ý nghĩa:** Theo dõi quy mô dữ liệu, tăng trưởng số lượng sản phẩm

### 8.5.2. Total Revenue (Tổng Doanh thu)

**Công thức:**
```sql
SELECT SUM(estimated_revenue) 
FROM products 
WHERE estimated_revenue IS NOT NULL;
```

**Logic:** Tổng doanh thu = SUM(sales_count × price) cho tất cả sản phẩm

**Ý nghĩa:** Ước tính tổng giá trị thị trường, theo dõi quy mô kinh doanh

### 8.5.3. Average Rating (Rating Trung bình)

**Công thức:**
```sql
SELECT AVG(rating_average) 
FROM products 
WHERE rating_average IS NOT NULL;
```

**Logic:** Trung bình cộng rating của tất cả sản phẩm có rating

**Ý nghĩa:** Đánh giá chất lượng sản phẩm tổng thể trên nền tảng

### 8.5.4. Average Sales Count (Số lượng Bán Trung bình)

**Công thức:**
```sql
SELECT AVG(sales_count) 
FROM products 
WHERE sales_count IS NOT NULL;
```

**Logic:** Trung bình số lượng đã bán của mỗi sản phẩm

**Ý nghĩa:** Đo lường độ phổ biến và nhu cầu thị trường

### 8.5.5. Popularity Score (Điểm Độ phổ biến)

**Công thức:**
```
popularity_score = (sales_count / max_sales) * 50 + 
                   (rating_avg / 5) * 30 + 
                   (review_count / max_reviews) * 20
```

**Logic:** Tính điểm từ 0-100 dựa trên sales (50%), rating (30%), reviews (20%)

**Ý nghĩa:** Đánh giá tổng hợp độ phổ biến của sản phẩm

### 8.5.6. Value Score (Điểm Giá trị)

**Công thức:**
```
value_score = rating_average / (price / 1,000,000)
```

**Logic:** Rating chia cho giá (triệu VND), sản phẩm có rating cao và giá thấp → điểm cao

**Ý nghĩa:** Xác định sản phẩm có giá trị tốt nhất (bang for the buck)

## 8.6. Storytelling Data

### 8.6.1. Story 1: "Sản phẩm Nào Bán Chạy Nhất?"

**Flow:**
1. **Overview:** Hiển thị Top 10 sản phẩm bán chạy (Bar Chart)
2. **Insight:** "Sản phẩm X có sales_count cao nhất với Y đơn vị"
3. **Drill-down:** Click vào sản phẩm → xem chi tiết (giá, rating, category)
4. **Context:** So sánh với sản phẩm tương tự trong cùng category
5. **Action:** Gợi ý sản phẩm tương tự có giá tốt hơn

### 8.6.2. Story 2: "Danh mục Nào Mang Lại Doanh thu Cao Nhất?"

**Flow:**
1. **Overview:** Top 10 categories theo doanh thu
2. **Insight:** "Category A có tổng doanh thu X tỷ VND"
3. **Drill-down:** Click category → xem tất cả sản phẩm trong category
4. **Analysis:** Phân tích giá trung bình, rating trung bình của category
5. **Comparison:** So sánh với category khác

### 8.6.3. Story 3: "Sản phẩm Nào Có Giá trị Tốt Nhất?"

**Flow:**
1. **Overview:** Top 20 sản phẩm theo value_score
2. **Insight:** "Sản phẩm có rating cao và giá hợp lý"
3. **Detail:** Hiển thị price, rating, value_score
4. **Recommendation:** Gợi ý cho người dùng tìm sản phẩm giá trị

### 8.6.4. Story 4: "Xu hướng Giá cả và Discount"

**Flow:**
1. **Overview:** Phân bố discount percent
2. **Insight:** "X% sản phẩm có discount > 20%"
3. **Analysis:** Mối quan hệ giữa discount và sales
4. **Trend:** (Nếu có dữ liệu lịch sử) Xu hướng giá theo thời gian

---

# 9. SCHEDULING & ORCHESTRATION

## 9.1. DAG (Airflow) gồm các Task

DAG `tiki_crawl_products_dag` được xây dựng trên Apache Airflow 3.1.2, bao gồm các task chính:

### 9.1.1. Task Groups

**1. Load and Prepare (TaskGroup: `load_and_prepare`)**
- **`load_categories`:** Load danh sách categories từ file JSON
- **`prepare_categories`:** Chuẩn bị danh sách categories để crawl

**2. Crawl Categories (TaskGroup: `crawl_categories`)**
- **`crawl_category_products`:** Crawl sản phẩm từ mỗi category (Dynamic Task Mapping)
  - Sử dụng Dynamic Task Mapping để crawl song song nhiều categories
  - Mỗi task crawl 1 category độc lập

**3. Process and Save (TaskGroup: `process_and_save`)**
- **`merge_products`:** Merge tất cả products từ các categories
- **`crawl_products_detail`:** Crawl chi tiết sản phẩm (Dynamic Task Mapping)
  - Sử dụng Selenium để crawl detail
  - Cache với Redis để tránh crawl lại
- **`save_products_raw`:** Lưu raw data vào JSON file
- **`transform_products`:** Transform và tính toán derived fields
- **`save_products_transformed`:** Lưu transformed data
- **`load_to_database`:** Load vào PostgreSQL

**4. Validate (TaskGroup: `validate`)**
- **`validate_data`:** Validate data quality
- **`aggregate_and_notify`:** Tổng hợp kết quả và gửi thông báo

### 9.1.2. Dependencies giữa các Tasks

```
load_categories → prepare_categories
prepare_categories → crawl_category_products (Dynamic)
crawl_category_products → merge_products
merge_products → crawl_products_detail (Dynamic)
crawl_products_detail → save_products_raw
save_products_raw → transform_products
transform_products → save_products_transformed
save_products_transformed → load_to_database
load_to_database → validate_data
validate_data → aggregate_and_notify
```

## 9.2. Lịch chạy (Daily/Hourly/Streaming)

### 9.2.1. Schedule Configuration

**Cấu hình linh hoạt qua Airflow Variable:**
- **Variable:** `TIKI_DAG_SCHEDULE_MODE`
- **Giá trị:**
  - `"scheduled"`: Chạy tự động hàng ngày
  - `"manual"`: Chỉ chạy khi trigger thủ công (mặc định)

**Schedule khi `scheduled`:**
```python
schedule = timedelta(days=1)  # Chạy hàng ngày
```

**Schedule khi `manual`:**
```python
schedule = None  # Không tự động chạy
```

### 9.2.2. Lịch chạy Đề xuất

**Daily Schedule (Khuyến nghị):**
- **Thời gian:** 02:00 AM mỗi ngày
- **Lý do:** 
  - Tránh giờ cao điểm
  - Dữ liệu được cập nhật sau khi ngày mới bắt đầu
  - Có đủ thời gian xử lý trước khi người dùng xem dashboard

**Hourly Schedule (Tùy chọn):**
- Có thể cấu hình chạy mỗi giờ cho các category quan trọng
- Sử dụng Dynamic Task Mapping để crawl chỉ một số categories

**Streaming (Tương lai):**
- Hiện tại: Batch processing
- Tương lai: Có thể implement streaming với Kafka + Airflow Sensors

## 9.3. Retry Strategy

### 9.3.1. Retry Configuration

**Default Args:**
```python
{
    "retries": 3,  # Retry 3 lần
    "retry_delay": timedelta(minutes=2),  # Delay 2 phút giữa các retry
    "retry_exponential_backoff": True,  # Exponential backoff
    "max_retry_delay": timedelta(minutes=10),  # Delay tối đa 10 phút
}
```

### 9.3.2. Retry Logic

**Exponential Backoff:**
- Lần 1: Retry sau 2 phút
- Lần 2: Retry sau 4 phút (2 × 2)
- Lần 3: Retry sau 8 phút (2 × 4)
- Tối đa: 10 phút

**Retry cho từng loại lỗi:**
- **Network errors:** Retry ngay lập tức (có thể do tạm thời)
- **Timeout errors:** Retry với delay
- **Parse errors:** Không retry (cần fix code)
- **Validation errors:** Không retry (dữ liệu không hợp lệ)

### 9.3.3. Circuit Breaker Pattern

**Implementation:**
- Sử dụng `CircuitBreaker` class để tránh retry quá nhiều khi service down
- **Threshold:** Sau 5 lỗi liên tiếp → mở circuit
- **Recovery:** Sau 30 phút → thử lại (half-open state)

## 9.4. Notification khi Pipeline Lỗi

### 9.4.1. Notification Channels

**1. Discord Webhook (Đã implement):**
- Gửi thông báo khi DAG hoàn thành (success/failed)
- Format: Markdown với emoji, tables, stats

**2. Airflow Email (Cấu hình sẵn):**
- `email_on_failure = False` (mặc định)
- Có thể bật bằng cách set `email_on_failure = True`

**3. Airflow UI:**
- Hiển thị trạng thái tasks trong Airflow UI
- Logs chi tiết cho từng task

### 9.4.2. Notification Content

**Khi Success:**
- Tổng số sản phẩm đã crawl
- Số categories đã xử lý
- Thời gian thực thi
- Stats (total revenue, avg rating, etc.)

**Khi Failed:**
- Task nào bị lỗi
- Error message
- Link đến Airflow UI để xem logs
- Suggestion để fix

### 9.4.3. Dead Letter Queue (DLQ)

**Implementation:**
- Failed tasks được lưu vào DLQ (Redis hoặc file)
- Có thể retry thủ công sau khi fix
- Log chi tiết để debug

## 9.5. Cơ chế Incremental Load

### 9.5.1. Strategy

**Full Load (Lần đầu):**
- Crawl tất cả sản phẩm từ tất cả categories
- Lưu vào database với `crawled_at` timestamp

**Incremental Load (Các lần sau):**
- **Option 1: Crawl tất cả, Update nếu thay đổi**
  - Crawl tất cả sản phẩm
  - So sánh với database (hash hoặc timestamp)
  - Chỉ update nếu có thay đổi

- **Option 2: Chỉ crawl sản phẩm mới**
  - Sử dụng cache để check sản phẩm đã crawl
  - Chỉ crawl sản phẩm chưa có trong database
  - **Vấn đề:** Không phát hiện sản phẩm đã thay đổi

### 9.5.2. Implementation

**Cache Mechanism:**
- **Redis Cache:** Lưu product_id đã crawl
- **TTL:** 24 giờ (hoặc configurable)
- **Check trước khi crawl:** Nếu có trong cache → skip

**Database Upsert:**
```sql
INSERT INTO products (...)
VALUES (...)
ON CONFLICT (product_id)
DO UPDATE SET
    name = EXCLUDED.name,
    price = EXCLUDED.price,
    ...
    updated_at = CURRENT_TIMESTAMP;
```

**Incremental Logic:**
1. Load danh sách product_id từ database
2. Với mỗi product mới crawl:
   - Nếu chưa có → INSERT
   - Nếu đã có → UPDATE nếu có thay đổi (so sánh hash)

### 9.5.3. Optimization

**Batch Processing:**
- Xử lý theo batch (100 products/batch) để tối ưu memory
- Commit transaction sau mỗi batch

**Parallel Processing:**
- Sử dụng Dynamic Task Mapping để crawl song song
- Mỗi task xử lý 1 category hoặc 1 batch products

---

# 10. MONITORING – DATA QUALITY – ERROR HANDLING

## 10.1. Monitoring Pipeline

### 10.1.1. Airflow Built-in Monitoring

**Airflow UI:**
- **DAGs View:** Xem trạng thái tất cả DAGs
- **Graph View:** Xem dependencies và trạng thái tasks
- **Tree View:** Xem lịch sử execution
- **Gantt Chart:** Xem thời gian thực thi tasks
- **Task Logs:** Xem logs chi tiết từng task

**Metrics:**
- Success rate: % tasks thành công
- Duration: Thời gian thực thi
- Retry count: Số lần retry

### 10.1.2. Custom Monitoring

**Logging:**
- **Structured Logging:** Sử dụng Python logging module
- **Log Levels:** DEBUG, INFO, WARNING, ERROR, CRITICAL
- **Log Format:** Timestamp, level, message, context

**Health Checks:**
- **Database Connection:** Check PostgreSQL connection
- **Redis Connection:** Check Redis connection
- **Service Health:** Check các services dependencies

**Metrics Collection (Tương lai):**
- Prometheus metrics
- Grafana dashboards
- Alerting rules

## 10.2. Data Quality

### 10.2.1. Duplicate Check

**Implementation:**
- **Database Level:** UNIQUE constraint trên `product_id`
- **Application Level:** Check duplicate trước khi insert
- **Upsert Logic:** `ON CONFLICT DO UPDATE` để update nếu duplicate

**Validation:**
```sql
-- Check duplicate products
SELECT product_id, COUNT(*) as count
FROM products
GROUP BY product_id
HAVING COUNT(*) > 1;
```

### 10.2.2. Schema Drift

**Detection:**
- **Expected Schema:** Định nghĩa schema mong đợi (Pydantic models hoặc JSON schema)
- **Validation:** Validate dữ liệu trước khi load vào database
- **Alert:** Gửi alert nếu schema thay đổi

**Handling:**
- Log schema drift để review
- Có thể auto-add columns nếu safe
- Manual review cho breaking changes

### 10.2.3. Freshness Check

**Implementation:**
- **Expected Freshness:** Dữ liệu phải được cập nhật trong 24 giờ
- **Check:** So sánh `crawled_at` với thời gian hiện tại
- **Alert:** Nếu dữ liệu quá cũ (> 24 giờ)

**Query:**
```sql
-- Check data freshness
SELECT 
    COUNT(*) as stale_count,
    MAX(crawled_at) as last_crawl
FROM products
WHERE crawled_at < NOW() - INTERVAL '24 hours';
```

### 10.2.4. Null Value Rules

**Rules:**
- **Required Fields:** `product_id`, `name`, `url` không được NULL
- **Optional Fields:** `price`, `rating_average`, `sales_count` có thể NULL
- **Validation:** Check required fields trước khi insert

**Handling:**
- **Required Fields:** Reject record nếu NULL
- **Optional Fields:** Để NULL hoặc set default value
- **Log:** Ghi log số lượng records có NULL values

**Query:**
```sql
-- Check null values
SELECT 
    COUNT(*) FILTER (WHERE product_id IS NULL) as null_product_id,
    COUNT(*) FILTER (WHERE name IS NULL) as null_name,
    COUNT(*) FILTER (WHERE price IS NULL) as null_price
FROM products;
```

### 10.2.5. Data Completeness

**Metrics:**
- **Completeness Rate:** % records có đầy đủ required fields
- **Target:** >= 95%

**Check:**
```sql
-- Data completeness
SELECT 
    COUNT(*) as total,
    COUNT(product_id) as has_product_id,
    COUNT(name) as has_name,
    COUNT(price) as has_price,
    COUNT(rating_average) as has_rating,
    (COUNT(*) - COUNT(product_id)) * 100.0 / COUNT(*) as missing_product_id_pct
FROM products;
```

## 10.3. Logging

### 10.3.1. Log Levels

**DEBUG:** Chi tiết kỹ thuật, dùng để debug
- Ví dụ: "Parsing HTML element: <div class='product'>"

**INFO:** Thông tin quan trọng về quá trình xử lý
- Ví dụ: "Crawled 100 products from category X"

**WARNING:** Cảnh báo nhưng không ảnh hưởng đến kết quả
- Ví dụ: "Product X has missing price field"

**ERROR:** Lỗi nhưng có thể retry
- Ví dụ: "Failed to crawl product X: Connection timeout"

**CRITICAL:** Lỗi nghiêm trọng, cần can thiệp ngay
- Ví dụ: "Database connection failed"

### 10.3.2. Log Format

**Structured Format:**
```
[2024-01-01 12:00:00] [INFO] [task_id] Message with context: {key: value}
```

**Context Information:**
- Task ID
- DAG Run ID
- Execution Date
- Product ID (nếu có)
- Category URL (nếu có)

### 10.3.3. Log Storage

**Airflow Logs:**
- Lưu trong Airflow metadata database
- Có thể xem qua Airflow UI
- Retention: 30 ngày (có thể config)

**Application Logs:**
- Ghi vào file hoặc stdout
- Có thể forward đến log aggregation system (ELK, Splunk)

## 10.4. SLA, SLO, SLI

### 10.4.1. SLA (Service Level Agreement)

**SLA với Business:**
- **Data Freshness:** Dữ liệu được cập nhật trong vòng 24 giờ
- **Data Completeness:** >= 95% records có đầy đủ required fields
- **Pipeline Success Rate:** >= 99% DAG runs thành công

### 10.4.2. SLO (Service Level Objective)

**Internal SLOs:**
- **Pipeline Execution Time:** < 2 giờ cho full crawl
- **Task Success Rate:** >= 99.5%
- **Data Quality:** < 1% records có validation errors

### 10.4.3. SLI (Service Level Indicator)

**Metrics để đo SLO:**
- **Pipeline Duration:** Thời gian thực thi DAG
- **Task Success Rate:** % tasks thành công
- **Data Freshness:** Thời gian từ lúc crawl đến lúc load vào DB
- **Error Rate:** Số lỗi / tổng số tasks

**Tracking:**
- Log metrics vào database hoặc monitoring system
- Dashboard để theo dõi SLIs
- Alert khi SLI vi phạm SLO

## 10.5. Error Handling Flow

### 10.5.1. Error Classification

**Error Types:**
1. **Network Errors:** Connection timeout, DNS error
2. **Parse Errors:** HTML structure thay đổi, không parse được
3. **Validation Errors:** Dữ liệu không pass validation rules
4. **Database Errors:** Connection failed, constraint violation
5. **Business Logic Errors:** Logic error trong code

### 10.5.2. Error Handling Strategy

**1. Retry với Exponential Backoff:**
- Network errors → Retry 3 lần với exponential backoff
- Database errors → Retry với delay

**2. Skip và Continue:**
- Parse errors → Log và skip product, tiếp tục product khác
- Validation errors → Log và skip record

**3. Dead Letter Queue:**
- Failed tasks sau khi hết retries → Lưu vào DLQ
- Có thể retry thủ công sau khi fix

**4. Circuit Breaker:**
- Nếu service fail quá nhiều → Mở circuit, không retry nữa
- Sau thời gian recovery → Thử lại (half-open)

### 10.5.3. Error Flow Diagram

```
Error Occurred
    ↓
Classify Error Type
    ↓
Is Retryable?
    ├─ Yes → Retry với Exponential Backoff
    │         ↓
    │    Success?
    │    ├─ Yes → Continue
    │    └─ No → Max Retries?
    │            ├─ Yes → Add to DLQ
    │            └─ No → Retry again
    │
    └─ No → Skip và Log
            ↓
        Continue với next item
```

### 10.5.4. Graceful Degradation

**Levels:**
1. **Full:** Tất cả features hoạt động bình thường
2. **Reduced:** Một số features bị tắt (ví dụ: detail crawl)
3. **Minimal:** Chỉ crawl cơ bản (không crawl detail)
4. **Failed:** Dừng hoàn toàn

**Implementation:**
- Check service health trước khi crawl
- Nếu service down → Degrade level
- Log degradation level để monitor

---

# 11. SƠ ĐỒ TỔNG HỢP (DẠNG TEXT DIAGRAM)

## 11.1. Kiến trúc Tổng thể

```
┌─────────────────────────────────────────────────────────────────┐
│                        EXTERNAL SOURCE                           │
│                         Tiki.vn Website                         │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             │ HTTP/HTTPS, Selenium
                             ↓
┌─────────────────────────────────────────────────────────────────┐
│                    AIRFLOW ORCHESTRATION                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
│  │  Scheduler   │  │  API Server  │  │    Worker    │        │
│  │              │  │   (UI :8080) │  │              │        │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘        │
│         │                 │                  │                  │
│         └─────────────────┴──────────────────┘                  │
│                           │                                      │
│                    ┌──────▼──────┐                               │
│                    │ DAG: tiki_  │                               │
│                    │ crawl_      │                               │
│                    │ products    │                               │
│                    └──────┬──────┘                               │
└───────────────────────────┼──────────────────────────────────────┘
                            │
                            │ Tasks Execution
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                         ETL PIPELINE                             │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    │
│  │   EXTRACT    │───▶│  TRANSFORM   │───▶│     LOAD     │    │
│  │              │    │              │    │              │    │
│  │ - Crawl Cat  │    │ - Normalize   │    │ - PostgreSQL │    │
│  │ - Crawl Prod │    │ - Validate    │    │ - JSON Backup│    │
│  │ - Crawl Det │    │ - Compute     │    │              │    │
│  └──────────────┘    └──────────────┘    └──────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                      STORAGE LAYER                              │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  PostgreSQL  │  │    Redis     │  │  JSON Files   │       │
│  │  (Database)  │  │   (Cache)    │  │  (Raw/Proc)   │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────────┘
                            │
                            │
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│                    ANALYTICS & DASHBOARD                        │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Data Mart   │  │   SQL Query  │  │  Dashboard    │       │
│  │  (Views)     │  │   (Ad-hoc)   │  │  (Power BI)   │       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────────┘
```

## 11.2. Data Flow Diagram

```
Tiki.vn
  │
  ├─ Categories (HTTP/HTTPS)
  │   └─▶ data/raw/categories_recursive_optimized.json
  │
  ├─ Products List (HTTP/HTTPS)
  │   └─▶ data/raw/products/products.json
  │
  └─ Product Details (Selenium)
      └─▶ data/raw/products/products_with_detail.json
          │
          ├─▶ Transform
          │   └─▶ data/processed/products_transformed.json
          │       │
          │       └─▶ Load to PostgreSQL
          │           └─▶ products table
          │               │
          │               ├─▶ Data Mart (Views)
          │               │   ├─ products_mart
          │               │   ├─ sales_mart
          │               │   └─ pricing_mart
          │               │
          │               └─▶ Dashboard (Power BI)
          │                   ├─ Overview
          │                   ├─ Detail
          │                   └─ Drill-down
```

## 11.3. ETL Pipeline Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                      EXTRACT PHASE                          │
│                                                             │
│  Task: load_categories                                      │
│    └─▶ Load categories from JSON                           │
│                                                             │
│  Task: crawl_category_products (Dynamic)                   │
│    ├─▶ Category 1 → Products List                          │
│    ├─▶ Category 2 → Products List                          │
│    └─▶ Category N → Products List                          │
│                                                             │
│  Task: crawl_products_detail (Dynamic)                   │
│    ├─▶ Product 1 → Detail (Selenium)                      │
│    ├─▶ Product 2 → Detail (Selenium)                      │
│    └─▶ Product N → Detail (Selenium)                      │
│                                                             │
│  Output: Raw JSON Files                                     │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                     TRANSFORM PHASE                         │
│                                                             │
│  Task: transform_products                                   │
│    ├─ Normalize (text, numbers, dates)                     │
│    ├─ Validate (schema, business rules)                    │
│    ├─ Flatten (nested structures)                          │
│    └─ Compute (revenue, popularity_score, value_score)      │
│                                                             │
│  Output: Transformed JSON Files                             │
└─────────────────────────────────────────────────────────────┘
                            │
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                       LOAD PHASE                            │
│                                                             │
│  Task: load_to_database                                     │
│    ├─ Connect to PostgreSQL                                │
│    ├─ Batch Insert/Upsert (100 records/batch)              │
│    └─ Transaction Commit                                   │
│                                                             │
│  Output: PostgreSQL products table                          │
└─────────────────────────────────────────────────────────────┘
```

## 11.4. ERD (Entity Relationship Diagram)

```
┌─────────────────────┐
│     categories       │
├─────────────────────┤
│ id (PK)             │
│ category_id (UK)    │
│ name                │
│ url (UK)            │
│ image_url           │
│ parent_url          │
│ level               │
│ product_count       │
│ created_at          │
│ updated_at          │
└──────────┬──────────┘
           │
           │ 1:N (conceptual)
           │
┌──────────▼──────────┐
│      products       │
├─────────────────────┤
│ id (PK)             │
│ product_id (UK)     │
│ name                │
│ url                 │
│ category_url        │──┐
│ category_id         │  │
│ sales_count         │  │
│ price               │  │
│ original_price      │  │
│ discount_percent    │  │
│ rating_average      │  │
│ review_count        │  │
│ estimated_revenue   │  │
│ popularity_score    │  │
│ value_score         │  │
│ brand               │  │
│ seller_name         │  │
│ seller_id           │  │
│ crawled_at          │  │
│ updated_at          │  │
└─────────────────────┘  │
                         │
┌────────────────────────┴──────┐
│    crawl_history               │
├────────────────────────────────┤
│ id (PK)                        │
│ crawl_type                     │
│ category_url                   │
│ product_id                     │
│ status                         │
│ items_count                    │
│ error_message                  │
│ started_at                     │
│ completed_at                   │
└────────────────────────────────┘
```

## 11.5. Star Schema

```
                    ┌──────────────┐
                    │   products   │
                    │ (Fact Table) │
                    ├──────────────┤
                    │ product_id   │
                    │ sales_count  │── Measures
                    │ price        │
                    │ revenue      │
                    │ popularity   │
                    │ value_score  │
                    │              │
                    │ category_url │──┐
                    │ category_id  │  │
                    │ seller_id    │  │── Foreign Keys
                    │ brand        │  │   to Dimensions
                    │ crawled_at   │  │
                    └──────┬───────┘  │
                           │          │
        ┌──────────────────┼──────────┼──────────┐
        │                  │          │          │
        │                  │          │          │
┌───────▼───────┐  ┌───────▼──────┐  │  ┌───────▼──────┐
│ Dim_Category  │  │  Dim_Seller  │  │  │  Dim_Brand   │
├───────────────┤  ├──────────────┤  │  ├──────────────┤
│ category_id   │  │ seller_id    │  │  │ brand_id     │
│ category_name │  │ seller_name  │  │  │ brand_name   │
│ category_url  │  │ is_official  │  │  └──────────────┘
│ parent_id     │  └──────────────┘  │
│ level         │                    │
└───────────────┘                    │
                                      │
                              ┌───────▼──────┐
                              │  Dim_Time    │
                              ├──────────────┤
                              │ date_key     │
                              │ date         │
                              │ day, month   │
                              │ year, quarter│
                              └──────────────┘
```

## 11.6. Dashboard Flow

```
┌─────────────────────────────────────────────────────────────┐
│                    DASHBOARD OVERVIEW                       │
│                                                             │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐ │
│  │ Total    │  │ Revenue   │  │ Avg      │  │ Avg      │ │
│  │ Products │  │           │  │ Rating   │  │ Sales    │ │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘ │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Top 10 Products (Bar Chart)                        │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Category Distribution (Pie Chart)                    │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Top Categories by Revenue (Bar Chart)              │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ Click Category
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                    DETAIL DASHBOARD                          │
│                                                             │
│  Tab: Products │ Category │ Pricing │ Brand                │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Products Table (Sortable, Filterable)                │  │
│  │  ID │ Name │ Price │ Sales │ Rating │ ...            │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Category Analysis                                    │  │
│  │  - Performance Table                                  │  │
│  │  - Tree Visualization                                 │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
                            │
                            │ Click Product
                            ↓
┌─────────────────────────────────────────────────────────────┐
│                  DRILL-DOWN DASHBOARD                       │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Product Detail                                      │  │
│  │  - Image, Description, Specifications                │  │
│  │  - Price History (if available)                      │  │
│  │  - Comparison with Similar Products                  │  │
│  └─────────────────────────────────────────────────────┘  │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐  │
│  │  Category Products List                              │  │
│  │  - All products in selected category                 │  │
│  │  - Filter and Sort options                           │  │
│  └─────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

---

**KẾT THÚC BÁO CÁO**

