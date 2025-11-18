# Tiki Data Pipeline

## 1. Giới Thiệu Dự Án

Dự án **Tiki Data Pipeline** là hệ thống ETL (Extract, Transform, Load) hoàn chỉnh được phát triển để thu thập, xử lý và lưu trữ dữ liệu sản phẩm từ nền tảng thương mại điện tử Tiki.vn. Dự án được triển khai như một pipeline dữ liệu tự động, kết hợp các công nghệ hiện đại để đảm bảo hiệu suất, độ tin cậy và khả năng mở rộng.

Hệ thống được xây dựng với mục đích học thuật và nghiên cứu, tập trung vào việc áp dụng các kỹ thuật Data Engineering trong môi trường thực tế.

## 2. Mục Tiêu Dự Án

### 2.1 Mục Tiêu Chính
- Xây dựng hệ thống thu thập dữ liệu tự động từ website thương mại điện tử
- Triển khai pipeline ETL hoàn chỉnh với các giai đoạn Extract, Transform, Load
- Áp dụng các pattern thiết kế cho hệ thống phân tán và fault-tolerant
- Đảm bảo chất lượng dữ liệu thông qua validation và error handling

### 2.2 Mục Tiêu Kỹ Thuật
- Implement web scraping với Selenium và async programming
- Xây dựng data transformation pipeline với computed fields
- Thiết kế database schema tối ưu cho dữ liệu sản phẩm
- Triển khai orchestration với Apache Airflow
- Tích hợp caching và rate limiting để tối ưu hiệu suất

## 3. Kiến Trúc Hệ Thống

### 3.1 Tổng Quan Kiến Trúc
Hệ thống được thiết kế theo mô hình ETL pipeline với 3 giai đoạn chính:

```
🌐 Tiki.vn Website
    ↓
📥 Extract (Crawl)
    ↓
🔄 Transform (Process)
    ↓
📤 Load (Store)
    ↓
💾 PostgreSQL + JSON Storage
```

### 3.2 Chi Tiết Kiến Trúc

#### Extract Pipeline
- **Crawl Categories**: Thu thập danh mục sản phẩm đệ quy
- **Crawl Products**: Lấy danh sách sản phẩm theo danh mục
- **Crawl Details**: Thu thập thông tin chi tiết sản phẩm
- **Technologies**: Selenium WebDriver, aiohttp, BeautifulSoup4

#### Transform Pipeline
- **Data Normalization**: Chuẩn hóa format dữ liệu
- **Validation**: Kiểm tra tính hợp lệ của dữ liệu
- **Computed Fields**: Tính toán estimated_revenue, popularity_score, value_score
- **Technologies**: Python dataclasses, custom validators

#### Load Pipeline
- **Database Storage**: PostgreSQL với upsert operations
- **JSON Backup**: Lưu trữ dữ liệu dưới dạng JSON
- **Batch Processing**: Xử lý dữ liệu theo batch để tối ưu hiệu suất
- **Technologies**: psycopg2, JSON serialization

#### Orchestration Layer
- **Apache Airflow**: DAG orchestration với Dynamic Task Mapping
- **Celery Executor**: Distributed task execution
- **Redis**: Message broker và caching layer

## 4. Công Nghệ Sử Dụng

### 4.1 Core Technologies
- **Python 3.8+**: Ngôn ngữ chính với asyncio, typing, dataclasses
- **PostgreSQL 16**: Database chính cho lưu trữ dữ liệu
- **Redis 7.2**: Caching và message broker
- **Apache Airflow 3.1.3**: Workflow orchestration

### 4.2 Web Scraping Stack
- **Selenium WebDriver 4.0+**: Browser automation
- **aiohttp**: Asynchronous HTTP client
- **BeautifulSoup4**: HTML parsing
- **webdriver-manager**: Automatic driver management

### 4.3 Infrastructure
- **Docker & Docker Compose**: Containerization
- **Git**: Version control
- **GitHub**: Repository hosting

### 4.4 Additional Libraries
- **psycopg2**: PostgreSQL adapter
- **python-dotenv**: Environment management
- **ruff**: Code linting và formatting
- **pytest**: Unit testing

## 5. Các Thành Phần Chính

### 5.1 Extract Pipeline (`src/pipelines/crawl/`)

#### crawl_categories_recursive.py
- Thu thập danh mục sản phẩm theo cấu trúc cây
- Implement recursive crawling với depth control
- Output: categories_recursive_optimized.json

#### crawl_products.py
- Crawl danh sách sản phẩm theo từng danh mục
- Dynamic pagination handling
- Rate limiting và error recovery

#### crawl_products_detail.py
- Thu thập thông tin chi tiết sản phẩm
- Selenium automation với driver pooling
- Multi-level caching (Redis + file)

### 5.2 Transform Pipeline (`src/pipelines/transform/`)

#### transformer.py
- **DataTransformer class**: Core transformation logic
- **Validation**: Required fields, data types, business rules
- **Normalization**: Standardize formats, handle missing values
- **Computed Fields**:
  - `estimated_revenue`: price × sales_count
  - `popularity_score`: sales_count × rating_average
  - `value_score`: (rating_average / price) × 1000

### 5.3 Load Pipeline (`src/pipelines/load/`)

#### loader.py
- **DataLoader class**: Database operations
- **Batch Upserts**: PostgreSQL ON CONFLICT handling
- **Connection Pooling**: Optimized database connections
- **Error Handling**: Transaction rollback, retry logic

### 5.4 Orchestration (`airflow/dags/`)

#### tiki_crawl_products_dag.py
- **Dynamic Task Mapping**: Parallel processing theo categories
- **TaskGroups**: Logical grouping của tasks
- **XCom**: Data sharing giữa tasks
- **Asset Tracking**: Dataset dependencies

## 6. Kết Quả Đạt Được

### 6.1 Dữ Liệu Thu Thập
- **Categories**: 400+ danh mục với cấu trúc phân cấp
- **Products**: 10,000+ sản phẩm với thông tin đầy đủ
- **Coverage**: Toàn bộ danh mục chính của Tiki.vn

### 6.2 Chất Lượng Dữ Liệu
- **Validation Rate**: >95% dữ liệu hợp lệ sau transform
- **Completeness**: Required fields đầy đủ cho 90%+ records
- **Accuracy**: Computed fields chính xác theo business logic

### 6.3 Hiệu Suất Hệ Thống
- **Crawl Speed**: 100-200 products/minute với Selenium
- **Processing Time**: <5 phút cho 1000 products
- **Memory Usage**: <2GB peak với batch processing
- **Error Recovery**: 99% success rate với retry patterns

## 7. Hạn Chế Và Hướng Phát Triển

### 7.1 Hạn Chế Hiện Tại
- **Scalability**: Limited by single machine resources
- **Rate Limiting**: Subject to Tiki.vn anti-bot measures
- **Data Freshness**: No real-time updates
- **Error Handling**: Limited edge case coverage
- **Monitoring**: Basic logging, no advanced metrics

### 7.2 Hướng Phát Triển Tương Lai
- **Distributed Crawling**: Multi-node architecture
- **Real-time Pipeline**: Event-driven updates
- **Advanced Analytics**: ML-based product categorization
- **API Layer**: REST API cho data access
- **Dashboard**: Web UI cho data visualization
- **Cloud Deployment**: AWS/GCP integration

## 8. Kết Luận

Dự án Tiki Data Pipeline đã thành công trong việc xây dựng một hệ thống ETL hoàn chỉnh với các tính năng:
# Tiki Data Pipeline - Tóm Tắt Ngắn

## 1. Giới Thiệu
**Tiki Data Pipeline** là hệ thống ETL thu thập và xử lý dữ liệu sản phẩm từ Tiki.vn phục vụ phân tích và nghiên cứu Data Engineering. Mục tiêu: tự động hóa thu thập, chuẩn hóa và lưu trữ dữ liệu với độ tin cậy cao.

## 2. Mục Tiêu
- Xây dựng pipeline ETL hoàn chỉnh (Extract → Transform → Load)
- Tối ưu crawl & lưu trữ qua batching, caching, retry
- Nâng cao chất lượng dữ liệu (validation + computed fields)

## 2.1 Phạm Vi Hiện Tại
- Tập trung crawl duy nhất danh mục Nhà Cửa & Đời Sống (c1883): https://tiki.vn/nha-cua-doi-song/c1883
- Chỉ thu thập sản phẩm và chi tiết trong cây danh mục này.

## 3. Kiến Trúc Tổng Quan
```
Website → Crawl → Transform → Load → PostgreSQL (+ JSON backup)
```
Thành phần chính: Selenium + aiohttp (crawl), Python (transform), PostgreSQL (load), Airflow (orchestrate), Redis (cache/broker).

## 4. Thành Phần Chính
- `crawl_categories_recursive.py`: Danh mục đệ quy
- `crawl_products.py`: Sản phẩm theo danh mục
- `crawl_products_detail.py`: Chi tiết sản phẩm (giá, rating,...)
- `transformer.py`: Chuẩn hóa + tính `estimated_revenue`, `popularity_score`, `value_score`
- `loader.py`: Batch upsert vào PostgreSQL
- `tiki_crawl_products_dag.py`: Airflow DAG (Dynamic Task Mapping)

## 5. Công Nghệ
Python 3.8+, Selenium, aiohttp, BeautifulSoup4, PostgreSQL, Redis, Apache Airflow, Docker.

## 6. Hạn Chế & Hướng Phát Triển
Hạn chế: chưa real-time, phụ thuộc một máy, rate limiting từ nguồn. 
Tương lai: distributed crawling, event-driven updates, API truy xuất, ML phân loại nâng cao.

## 7. Kết Luận
Dự án chứng minh triển khai hiệu quả một ETL thực tế với khả năng mở rộng, tối ưu hiệu suất và bảo đảm chất lượng dữ liệu — làm nền tảng cho các bước phát triển tiếp theo.

## 8. Tác Giả
Nguyễn Hữu Cường  |  Python



