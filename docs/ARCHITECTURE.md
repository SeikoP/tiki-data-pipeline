# 📊 Tiki Data Pipeline - Architecture

## Tổng quan

Hệ thống Tiki Data Pipeline là một ETL pipeline hoàn chỉnh với Asset-aware scheduling, được xây dựng trên Apache Airflow 3.1.2.

## Kiến trúc hệ thống

### 1. External Source
- **Tiki.vn Website**: Nguồn dữ liệu chính
  - HTTP/HTTPS requests cho categories và products
  - Selenium WebDriver cho dynamic content

### 2. Airflow Orchestration
- **Airflow Scheduler**: Schedule và trigger DAGs
- **Airflow API Server**: Web UI và REST API (port 8080)
- **Airflow Worker**: Execute tasks
- **DAG Processor**: Parse và load DAGs

### 3. Storage Layer
- **PostgreSQL**: 
  - Airflow metadata database
  - Products data storage
- **Redis**:
  - Celery message broker
  - Multi-level cache

### 4. ETL Pipeline

#### Extract (Crawl)
- **Crawl Categories**: Recursive crawling danh mục
- **Crawl Products**: Dynamic Task Mapping cho parallel crawling
- **Crawl Details**: Selenium-based detail extraction

#### Transform
- **Transform Products**: Normalize, validate dữ liệu
- **Compute Fields**: Tính toán revenue, popularity score, value score

#### Load
- **Load to Database**: PostgreSQL + JSON backup
- Batch processing với upsert support

### 5. Data Storage
- **Raw Data**: JSON files từ crawl
- **Processed Data**: JSON files sau transform
- **PostgreSQL Table**: Products table trong database

### 6. Asset Tracking
- `tiki://products/raw`: Raw products dataset
- `tiki://products/with_detail`: Products với chi tiết dataset
- `tiki://products/transformed`: Transformed products dataset
- `tiki://products/final`: Final products dataset

### 7. Validation & Analytics
- **Validate Data**: Kiểm tra data quality
- **Aggregate & Notify**: Tổng hợp và gửi thông báo

## Data Flow

```
Tiki.vn
  ↓ (HTTP/HTTPS, Selenium)
Extract (Crawl)
  ↓ (Raw JSON + Asset: tiki://products/raw, tiki://products/with_detail)
Transform
  ↓ (Transformed JSON + Asset: tiki://products/transformed)
Load
  ↓ (PostgreSQL + JSON + Asset: tiki://products/final)
Validate & Aggregate
```

## Asset Dependencies

Assets được sử dụng để trigger các DAGs downstream:

- `tiki://products/with_detail` → Triggers Transform DAG
- `tiki://products/transformed` → Triggers Load DAG
- `tiki://products/final` → Triggers Validation DAG

## Diagram Files

Các file diagram có sẵn trong thư mục `docs/`:

1. **architecture.mmd** - Mermaid format
   - Import vào: [Mermaid Live Editor](https://mermaid.live)
   - VS Code với Mermaid extension
   - GitHub (hiển thị tự động)

2. **architecture.puml** - PlantUML format
   - Import vào: [PlantUML Online](http://www.plantuml.com/plantuml/uml/)
   - IntelliJ IDEA
   - VS Code với PlantUML extension

3. **architecture.drawio.xml** - Draw.io format
   - Import vào: [Draw.io](https://app.diagrams.net/)
   - Mở file XML trong Draw.io để chỉnh sửa

## Cách sử dụng Diagram Files

### Mermaid (architecture.mmd)
```bash
# Online
# Mở https://mermaid.live và paste nội dung file

# VS Code
# Cài extension "Markdown Preview Mermaid Support"
# Mở file .mmd và preview
```

### PlantUML (architecture.puml)
```bash
# Online
# Mở http://www.plantuml.com/plantuml/uml/
# Paste nội dung file

# VS Code
# Cài extension "PlantUML"
# Mở file .puml và preview
```

### Draw.io (architecture.drawio.xml)
```bash
# Online
# Mở https://app.diagrams.net/
# File → Open from → Device → Chọn file .xml

# Desktop
# Tải Draw.io desktop app
# Mở file .xml
```

## Technology Stack

- **Orchestration**: Apache Airflow 3.1.2
- **Web Scraping**: Selenium WebDriver 4.0+, BeautifulSoup4
- **Databases**: PostgreSQL 16, Redis 7.2
- **Containerization**: Docker, Docker Compose
- **Language**: Python 3.8+
- **Data Format**: JSON

## Performance Considerations

- **Parallel Processing**: Dynamic Task Mapping cho crawl song song
- **Caching**: Multi-level cache (Redis + File) để giảm requests
- **Batch Processing**: Xử lý dữ liệu theo batch để tối ưu memory
- **Rate Limiting**: Delay giữa requests để tránh bị block
- **Resource Limits**: Giới hạn CPU và memory cho từng service

## Scalability

Hệ thống có thể scale bằng cách:
- Tăng số Airflow workers
- Sử dụng Celery executor với multiple workers
- Tăng database connection pool
- Sử dụng Redis cluster cho cache
- Horizontal scaling với Kubernetes (nếu cần)

