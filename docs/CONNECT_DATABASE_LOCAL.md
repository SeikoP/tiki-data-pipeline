# Kết nối Database từ Máy Local

Hướng dẫn kết nối đến PostgreSQL database trong Docker từ máy local 

## 📋 Yêu cầu

1. **PostgreSQL container đang chạy**:
   ```bash
   docker compose ps postgres
   ```

2. **Port 5432 đã được expose** (đã được thêm vào `docker-compose.yaml`):
   ```yaml
   postgres:
     ports:
       - "5432:5432"
   ```

3. **Cài đặt psycopg2** (nếu chưa có):
   ```bash
   pip install psycopg2-binary
   ```

## 🔌 Thông tin kết nối

### Từ file `.env`:
- **Host**: `localhost` (hoặc `127.0.0.1`)
- **Port**: `5432`
- **User**: Lấy từ `POSTGRES_USER` trong `.env` (mặc định: `postgres`)
- **Password**: Lấy từ `POSTGRES_PASSWORD` trong `.env` (mặc định: `postgres`)
- **Database**: 
  - `crawl_data` - Database cho dữ liệu crawl
  - `airflow` - Database cho Airflow metadata

## 🧪 Test kết nối

### 1. Sử dụng script test:

```bash
python scripts/test_postgres_local.py
```

Script này sẽ:
- Tự động đọc thông tin từ `.env`
- Test 4 cách kết nối khác nhau
- Hiển thị thông tin kết nối và cách sử dụng

### 2. Test bằng Python code:

```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="crawl_data",
    user="postgres",
    password="postgres",
    connect_timeout=10,
)

# Test query
with conn.cursor() as cur:
    cur.execute("SELECT version();")
    version = cur.fetchone()
    print(f"PostgreSQL version: {version[0]}")

conn.close()
```

### 3. Test bằng psql (command line):

**Windows PowerShell:**
```powershell
$env:PGPASSWORD="postgres"
psql -h localhost -p 5432 -U postgres -d crawl_data
```

**Linux/Mac:**
```bash
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -d crawl_data
```

## 💻 Sử dụng trong Code

### 1. Sử dụng PostgresStorage:

```python
from pipelines.crawl.storage.postgres_storage import PostgresStorage

# Kết nối từ máy local
storage = PostgresStorage(
    host="localhost",        # hoặc "127.0.0.1"
    port=5432,
    database="crawl_data",   # hoặc "airflow"
    user="postgres",          # Lấy từ .env
    password="postgres",      # Lấy từ .env
)

# Sử dụng storage
with storage.get_connection() as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM products LIMIT 10;")
        results = cur.fetchall()
        print(results)

storage.close()
```

### 2. Sử dụng psycopg2 trực tiếp:

```python
import psycopg2
from psycopg2.extras import Json

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="crawl_data",
    user="postgres",
    password="postgres",
)

# Hoặc dùng connection string
conn_str = "postgresql://postgres:postgres@localhost:5432/crawl_data"
conn = psycopg2.connect(conn_str)
```

### 3. Sử dụng với environment variables:

```python
import os
from pipelines.crawl.storage.postgres_storage import PostgresStorage

storage = PostgresStorage(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=int(os.getenv("POSTGRES_PORT", "5432")),
    database=os.getenv("POSTGRES_DB", "crawl_data"),
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
)
```

## 🔧 Xử lý lỗi

### Lỗi: "Connection refused" hoặc "Connection timeout"

**Nguyên nhân:**
1. PostgreSQL container chưa chạy
2. Port chưa được expose
3. Port 5432 đã được sử dụng bởi PostgreSQL local khác
4. Firewall chặn kết nối

**Giải pháp:**
1. Kiểm tra container: `docker compose ps postgres`
2. Khởi động lại container: `docker compose up -d postgres`
3. Kiểm tra port mapping trong `docker-compose.yaml`
4. Nếu port 5432 đã được dùng, đổi port mapping:
   ```yaml
   ports:
     - "5433:5432"  # Map port 5433 (local) -> 5432 (container)
   ```
   Khi đó dùng `port=5433` khi kết nối.

### Lỗi: "Authentication failed"

**Nguyên nhân:**
- Username/password không đúng

**Giải pháp:**
1. Kiểm tra `.env` file
2. Reset password nếu cần: `scripts/reset_postgres_password.ps1`

### Lỗi: "Database does not exist"

**Nguyên nhân:**
- Database chưa được tạo

**Giải pháp:**
1. Database `crawl_data` và `airflow` sẽ được tạo tự động khi container khởi động lần đầu
2. Nếu chưa có, tạo thủ công:
   ```sql
   CREATE DATABASE crawl_data;
   ```

## 📝 Connection String Formats

### 1. PostgreSQL URI:
```
postgresql://user:password@host:port/database
```
Ví dụ:
```
postgresql://postgres:postgres@localhost:5432/crawl_data
```

### 2. DSN (Data Source Name):
```
host=localhost port=5432 dbname=crawl_data user=postgres password=postgres
```

### 3. SQLAlchemy (cho Airflow):
```
postgresql+psycopg2://user:password@host:port/database
```

## 🔐 Bảo mật

⚠️ **Lưu ý:** 
- Không commit file `.env` chứa password vào Git
- Sử dụng password mạnh cho production
- Chỉ expose port khi cần thiết (development)
- Trong production, cân nhắc không expose port ra ngoài

## 📚 Tài liệu tham khảo

- [PostgreSQL Documentation](https://www.postgresql.org/docs/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)
- [Docker Compose Networking](https://docs.docker.com/compose/networking/)

