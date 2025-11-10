# 🚀 Data Pipeline Template

Một **template repository** hoàn chỉnh để chạy **Apache Airflow** kết hợp với **Firecrawl Self-Host** cho các dự án data pipeline.

> 💡 **Template này có thể được sử dụng lại cho nhiều dự án khác nhau!**

## 📌 Sử dụng như Template

Xem file [TEMPLATE.md](docs/TEMPLATE.md) để biết cách sử dụng repository này như một template cho dự án mới.

## 🎯 Tính năng

- ✅ **Apache Airflow 3.1.2** - Workflow orchestration
- ✅ **Firecrawl Self-Host** - Web scraping và crawling
- ✅ **Shared Databases** - Tối ưu tài nguyên với 1 Redis + 1 Postgres
- ✅ **Docker Compose** - Dễ dàng deploy và quản lý
- ✅ **Resource Limits** - Quản lý tài nguyên hiệu quả
- ✅ **Health Checks** - Tự động kiểm tra sức khỏe services

## 📋 Yêu cầu

- **Docker** >= 20.10
- **Docker Compose** >= 2.0
- **RAM**: Tối thiểu 4GB (khuyến nghị 8GB+)
- **CPU**: Tối thiểu 2 cores
- **Disk**: Tối thiểu 10GB trống

## 🚀 Quick Start

### 1. Clone repository

```bash
git clone [<repository-url>](https://github.com/SeikoP/airflow-firecrawl-data-pipeline)
cd airflow-firecrawl-data-pipeline
```

### 2. Cấu hình môi trường

```bash
# Copy file mẫu
cp .env.example .env

# Chỉnh sửa các biến môi trường cần thiết
# Đặc biệt là: OPENAI_API_KEY, BULL_AUTH_KEY, TEST_API_KEY
nano .env  # hoặc dùng editor khác
```

### 3. Khởi động services

```bash
# Build và khởi động tất cả services
docker-compose up -d

# Xem logs
docker-compose logs -f

# Kiểm tra trạng thái
docker-compose ps
```

### 4. Truy cập services

- **Airflow Web UI**: http://localhost:8080
  - Username: `airflow` (mặc định)
  - Password: `airflow` (mặc định)
  
- **Firecrawl API**: http://localhost:3002
  - API documentation: http://localhost:3002/docs

## 📁 Cấu trúc dự án

```
tiki-data-pipeline/
├── docker-compose.yaml          # Cấu hình chính (shared databases)
├── docker-compose.separate-db.yaml  # Backup: tách riêng databases
├── .env.example                 # Template biến môi trường
├── scripts/
│   └── init-multiple-databases.sh   # Script tạo databases
├── airflow/
│   ├── dags/                    # Airflow DAGs của bạn
│   ├── logs/                    # Airflow logs (gitignored)
│   ├── config/                  # Airflow config
│   └── plugins/                 # Airflow plugins
├── firecrawl/                   # Firecrawl source code
└── src/                        # Source code dự án của bạn
    ├── pipelines/
    ├── models/
    └── utils/
```

## 🗄️ Cấu trúc Databases

### Redis (Shared)
- **Database 0**: Airflow Celery broker
- **Database 1**: Firecrawl queue & rate limiting

### Postgres (Shared)
- **Database `airflow`**: User `airflow`, password `airflow`
- **Database `nuq`**: User `postgres`, password `postgres`

> 💡 **Lưu ý**: Cấu hình này tối ưu cho development/staging. Production nên cân nhắc tách riêng databases.

## ⚙️ Cấu hình

### Biến môi trường quan trọng

Xem file `.env.example` để biết danh sách đầy đủ. Các biến quan trọng nhất:

```bash
# Bắt buộc
OPENAI_API_KEY=your_key_here
BULL_AUTH_KEY=your_key_here
TEST_API_KEY=your_key_here

# Tùy chọn nhưng khuyến nghị
AIRFLOW_UID=50000  # Linux: id -u
```

### Resource Limits

Các services đã được cấu hình resource limits để tránh chiếm quá nhiều tài nguyên:

- **Postgres**: 1 CPU, 1GB RAM
- **Redis**: 0.5 CPU, 512MB RAM
- **Airflow Services**: 0.5-2 CPU, 256MB-2GB RAM
- **Firecrawl Services**: 0.5-2 CPU, 512MB-2GB RAM

Bạn có thể điều chỉnh trong `docker-compose.yaml` nếu cần.

## 🔧 Sử dụng

### Tạo DAG mới

```bash
# Tạo file DAG trong airflow/dags/
nano airflow/dags/my_dag.py
```

Ví dụ DAG đơn giản:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello from Airflow!")

with DAG(
    'my_first_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:
    task = PythonOperator(
        task_id='hello',
        python_callable=hello_world
    )
```

### Sử dụng Firecrawl API

```python
import requests

# Scrape một website
response = requests.post(
    'http://localhost:3002/v0/scrape',
    json={
        'url': 'https://example.com',
        'formats': ['markdown']
    },
    headers={'Authorization': f'Bearer {TEST_API_KEY}'}
)
print(response.json())
```

### Backup dữ liệu

```bash
# Backup Airflow database
docker-compose exec postgres pg_dump -U airflow airflow > airflow_backup.sql

# Backup Firecrawl database
docker-compose exec postgres pg_dump -U postgres nuq > nuq_backup.sql
```

### Restore dữ liệu

```bash
# Restore Airflow
docker-compose exec -T postgres psql -U airflow -d airflow < airflow_backup.sql

# Restore Firecrawl
docker-compose exec -T postgres psql -U postgres -d nuq < nuq_backup.sql
```

## 🐛 Troubleshooting

### Services không khởi động

```bash
# Xem logs chi tiết
docker-compose logs [service-name]

# Kiểm tra health status
docker-compose ps

# Restart service
docker-compose restart [service-name]
```

### Lỗi database connection

```bash
# Kiểm tra Postgres đã sẵn sàng
docker-compose exec postgres pg_isready -U postgres

# Kiểm tra databases đã được tạo
docker-compose exec postgres psql -U postgres -c "\l"
```

### Lỗi permissions (Linux)

```bash
# Set AIRFLOW_UID
export AIRFLOW_UID=$(id -u)
echo "AIRFLOW_UID=$AIRFLOW_UID" >> .env
```

### Xóa và khởi động lại

```bash
# Dừng và xóa containers, volumes
docker-compose down -v

# Khởi động lại từ đầu
docker-compose up -d
```

## 📚 Tài liệu tham khảo

### Tài liệu trong repository
- [Documentation Index](docs/README.md) - Tổng quan tài liệu
- [QUICK_START.md](docs/QUICK_START.md) - Hướng dẫn nhanh
- [TEMPLATE.md](docs/TEMPLATE.md) - Cách sử dụng template
- [SETUP_GITHUB.md](docs/SETUP_GITHUB.md) - Setup GitHub

### Tài liệu bên ngoài
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Firecrawl Self-Host Guide](https://docs.firecrawl.dev/self-hosting)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

## 🔄 Migration từ Separate Databases

Nếu bạn đang dùng `docker-compose.separate-db.yaml` và muốn chuyển sang shared databases:

```bash
# 1. Backup dữ liệu
docker-compose -f docker-compose.separate-db.yaml exec postgres pg_dump -U airflow airflow > airflow_backup.sql
docker-compose -f docker-compose.separate-db.yaml exec nuq-postgres pg_dump -U postgres postgres > nuq_backup.sql

# 2. Dừng containers cũ
docker-compose -f docker-compose.separate-db.yaml down

# 3. Khởi động với cấu hình mới
docker-compose up -d

# 4. Restore dữ liệu
docker-compose exec -T postgres psql -U airflow -d airflow < airflow_backup.sql
docker-compose exec -T postgres psql -U postgres -d nuq < nuq_backup.sql
```

## 📝 License

Xem file LICENSE trong repository.

## 📚 Tài liệu

Xem thư mục [docs/](docs/) để biết thêm chi tiết:
- [QUICK_START.md](docs/QUICK_START.md) - Hướng dẫn nhanh push template
- [TEMPLATE.md](docs/TEMPLATE.md) - Cách sử dụng template
- [SETUP_GITHUB.md](docs/SETUP_GITHUB.md) - Setup GitHub template
- [CONTRIBUTING.md](docs/CONTRIBUTING.md) - Contributing guidelines

## 🤝 Contributing

Contributions are welcome! Vui lòng tạo issue hoặc pull request. Xem [CONTRIBUTING.md](docs/CONTRIBUTING.md) để biết thêm chi tiết.

## ⚠️ Lưu ý

- File `.env` chứa thông tin nhạy cảm, **KHÔNG** commit lên Git
- Production: Nên thay đổi mật khẩu mặc định và sử dụng secrets management
- Production: Cân nhắc tách riêng databases nếu cần isolation cao

## 📞 Support

Nếu gặp vấn đề, vui lòng tạo issue trên GitHub repository.

