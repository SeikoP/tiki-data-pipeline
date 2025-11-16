# PostgreSQL Backup Script

Script để backup PostgreSQL database vào thư mục `backups/postgres`.

## Cách sử dụng

### 1. Backup tất cả databases

```bash
# Python script (khuyến nghị)
python scripts/helper/backup_postgres.py

# Hoặc PowerShell
.\scripts\backup-postgres.ps1

# Hoặc Bash
bash scripts/backup-postgres.sh
```

### 2. Backup một database cụ thể

```bash
# Python script
python scripts/helper/backup_postgres.py --database crawl_data
python scripts/helper/backup_postgres.py --database airflow

# PowerShell
.\scripts\backup-postgres.ps1 -Database crawl_data

# Bash
bash scripts/backup-postgres.sh crawl_data
```

### 3. Chọn format backup

```bash
# Custom format (nhỏ nhất, khuyến nghị)
python scripts/helper/backup_postgres.py --format custom

# SQL format (dễ đọc, lớn hơn)
python scripts/helper/backup_postgres.py --format sql

# Tar format
python scripts/helper/backup_postgres.py --format tar
```

### 4. Liệt kê các file backup

```bash
python scripts/helper/backup_postgres.py --list
```

## Format backup

- **custom** (`.dump`): Format nhị phân của PostgreSQL, nhỏ nhất, nhanh nhất (khuyến nghị)
- **sql** (`.sql`): SQL text, dễ đọc và chỉnh sửa, nhưng lớn hơn
- **tar** (`.tar`): Tar archive, có thể nén

## Thư mục backup

Backup files được lưu tại: `E:\Project\tiki-data-pipeline\backups\postgres`

Format tên file: `{database}_{timestamp}.{ext}`

Ví dụ:
- `crawl_data_20251116_202329.dump`
- `airflow_20251116_202329.sql`

## Restore backup

Để restore backup, sử dụng script `restore-postgres.ps1` hoặc `restore-postgres.sh`:

```bash
# PowerShell
.\scripts\restore-postgres.ps1 -Database crawl_data -BackupFile backups/postgres/crawl_data_20251116_202329.dump

# Bash
bash scripts/restore-postgres.sh crawl_data backups/postgres/crawl_data_20251116_202329.dump
```

## Lưu ý

1. **Container phải đang chạy**: Script sẽ kiểm tra container PostgreSQL có đang chạy không
2. **File .env**: Script cần file `.env` để lấy thông tin kết nối database
3. **Quyền truy cập**: Đảm bảo có quyền ghi vào thư mục `backups/postgres`

## Tự động backup

Có thể tích hợp vào DAG để backup tự động định kỳ. Ví dụ:

```python
from airflow.providers.standard.operators.python import PythonOperator

def backup_database(**context):
    import subprocess
    subprocess.run([
        "python", "scripts/helper/backup_postgres.py",
        "--database", "crawl_data"
    ])

backup_task = PythonOperator(
    task_id="backup_database",
    python_callable=backup_database,
    dag=dag
)
```

