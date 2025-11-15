# Tại sao không có dữ liệu trong Database?

## 🔍 Nguyên nhân

Luồng Airflow không load dữ liệu vào database vì **database credentials không khớp**.

### Vấn đề:

1. **DAG sử dụng default values không đúng**:
   - Code trong DAG: `default_var="airflow"` cho user và password
   - Database thực tế: `postgres`/`postgres` (từ file `.env`)

2. **Airflow Variables chưa được set**:
   - Nếu Airflow Variables không được set, code sẽ dùng default values
   - Default values (`airflow`/`airflow`) không khớp với database thực tế

3. **Kết quả**:
   - Task `load_products` chạy nhưng không thể kết nối database
   - Hoặc kết nối được nhưng authentication failed
   - Dữ liệu không được lưu vào database

## ✅ Giải pháp

### Cách 1: Set Airflow Variables (Khuyến nghị)

Set các biến sau trong Airflow UI (Admin → Variables):

```
POSTGRES_HOST = postgres
POSTGRES_PORT = 5432
POSTGRES_DB = crawl_data
POSTGRES_USER = postgres
POSTGRES_PASSWORD = postgres
```

**Hoặc dùng CLI:**

```bash
docker compose exec airflow-scheduler airflow variables set POSTGRES_USER postgres
docker compose exec airflow-scheduler airflow variables set POSTGRES_PASSWORD postgres
docker compose exec airflow-scheduler airflow variables set POSTGRES_HOST postgres
docker compose exec airflow-scheduler airflow variables set POSTGRES_PORT 5432
docker compose exec airflow-scheduler airflow variables set POSTGRES_DB crawl_data
```

### Cách 2: Sửa Code (Đã được fix)

Code đã được cập nhật để:
1. Ưu tiên lấy từ Airflow Variables
2. Nếu không có, lấy từ Environment Variables (từ `.env` file)
3. Cuối cùng mới dùng default values

```python
db_user = Variable.get("POSTGRES_USER", default_var=os.getenv("POSTGRES_USER", "postgres"))
db_password = Variable.get("POSTGRES_PASSWORD", default_var=os.getenv("POSTGRES_PASSWORD", "postgres"))
```

### Cách 3: Load dữ liệu thủ công

Nếu DAG đã chạy và có dữ liệu trong file, có thể load thủ công:

```bash
python scripts/load_data_to_db.py
```

## 🔍 Kiểm tra

### 1. Kiểm tra Airflow Variables:

```bash
docker compose exec airflow-scheduler airflow variables list | grep POSTGRES
```

### 2. Kiểm tra database connection từ Airflow:

```bash
docker compose exec airflow-scheduler python scripts/test_postgres_connection.py
```

### 3. Kiểm tra dữ liệu trong database:

```bash
docker compose exec -T postgres psql -U postgres -d crawl_data -c "SELECT COUNT(*) FROM products;"
```

## 📝 Lưu ý

1. **Airflow Variables vs Environment Variables**:
   - Airflow Variables: Set trong Airflow UI, ưu tiên cao nhất
   - Environment Variables: Từ `.env` file, được load vào container
   - Default values: Chỉ dùng khi cả 2 trên đều không có

2. **Database credentials**:
   - Phải khớp với credentials trong `.env` file
   - Nếu đã thay đổi password, cần update cả Airflow Variables

3. **Sau khi fix**:
   - Chạy lại DAG hoặc trigger task `load_products`
   - Hoặc dùng script `load_data_to_db.py` để load dữ liệu hiện có

## 🚀 Sau khi fix

Sau khi set đúng credentials, DAG sẽ tự động load dữ liệu vào database khi chạy task `load_products`.

Kiểm tra logs của task `load_products` để xem kết quả:

```bash
# Xem logs của task load_products
docker compose exec airflow-scheduler airflow tasks logs tiki_crawl_products load_products <run_id>
```

