# Hướng Dẫn Xử Lý Sự Cố DAG

## Tóm Tắt Vấn Đề

Từ log của DAG `tiki_crawl_products`, có các vấn đề sau:

### 1. DAG Đang Chạy Nhưng Bị Giới Hạn

**Vấn đề:** DAG đang chạy nhưng bị giới hạn bởi `max_active_tasks = 10`. Nhiều tasks đang chờ trong queue.

**Log từ scheduler:**
```
Not executing <TaskInstance: ...> since the number of tasks running or queued from DAG tiki_crawl_products is >= to the DAG's max_active_tasks limit of 10
```

**Giải pháp:**
- Tăng `max_active_tasks` trong DAG config nếu muốn chạy nhiều tasks song song hơn
- Hoặc chờ các tasks hiện tại hoàn thành

### 2. Thiếu Airflow Variables

**Vấn đề:** Nhiều Airflow Variables bị thiếu, gây ra nhiều log errors (nhưng không chặn DAG chạy vì có default values).

**Các Variables bị thiếu:**
- `TIKI_DAG_SCHEDULE_MODE`
- `TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD`
- `TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT`
- `REDIS_URL`
- `TIKI_DEGRADATION_FAILURE_THRESHOLD`
- `TIKI_DEGRADATION_RECOVERY_THRESHOLD`
- `TIKI_MAX_PAGES_PER_CATEGORY`
- `TIKI_USE_SELENIUM`
- `TIKI_CRAWL_TIMEOUT`
- `TIKI_RATE_LIMIT_DELAY`

**Giải pháp:** Chạy script setup variables:

```bash
# Cách 1: Dùng script Python
docker-compose exec airflow-scheduler python /opt/airflow/tests/setup_airflow_variables.py

# Cách 2: Dùng Airflow CLI
docker-compose exec airflow-scheduler airflow variables set TIKI_DAG_SCHEDULE_MODE manual
docker-compose exec airflow-scheduler airflow variables set TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD 5
docker-compose exec airflow-scheduler airflow variables set TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT 60
docker-compose exec airflow-scheduler airflow variables set REDIS_URL redis://redis:6379/3
docker-compose exec airflow-scheduler airflow variables set TIKI_DEGRADATION_FAILURE_THRESHOLD 3
docker-compose exec airflow-scheduler airflow variables set TIKI_DEGRADATION_RECOVERY_THRESHOLD 5
docker-compose exec airflow-scheduler airflow variables set TIKI_MAX_PAGES_PER_CATEGORY 0
docker-compose exec airflow-scheduler airflow variables set TIKI_USE_SELENIUM false
docker-compose exec airflow-scheduler airflow variables set TIKI_CRAWL_TIMEOUT 300
docker-compose exec airflow-scheduler airflow variables set TIKI_RATE_LIMIT_DELAY 1.0
```

### 3. DAG Schedule Mode

**Vấn đề:** DAG đang ở chế độ `manual` (schedule = None), nghĩa là chỉ chạy khi trigger thủ công.

**Giải pháp:** 
- Để chạy tự động hàng ngày, set variable:
  ```bash
  docker-compose exec airflow-scheduler airflow variables set TIKI_DAG_SCHEDULE_MODE scheduled
  ```
- Sau đó restart DAG processor để áp dụng thay đổi:
  ```bash
  docker-compose restart airflow-dag-processor
  ```

## Cách Xem Log DAG

### 1. Xem Log Scheduler
```bash
docker-compose logs airflow-scheduler --tail 100
```

### 2. Xem Log DAG Processor
```bash
docker-compose logs airflow-dag-processor --tail 100
```

### 3. Xem Log Worker
```bash
docker-compose logs airflow-worker --tail 100
```

### 4. Xem Log Của Task Cụ Thể
```bash
# Xem log file trực tiếp
cat airflow/logs/dag_id=tiki_crawl_products/run_id=<run_id>/task_id=<task_id>/attempt=1.log

# Hoặc dùng Airflow CLI
docker-compose exec airflow-scheduler airflow tasks logs tiki_crawl_products <task_id> <run_id>
```

### 5. Xem Log DAG Processor (Parse Errors)
```bash
cat airflow/logs/dag_processor/2025-11-16/dags-folder/tiki_crawl_products_dag.py.log
```

## Kiểm Tra Trạng Thái DAG

### 1. Xem Danh Sách DAGs
```bash
docker-compose exec airflow-scheduler airflow dags list
```

### 2. Xem Chi Tiết DAG
```bash
docker-compose exec airflow-scheduler airflow dags show tiki_crawl_products
```

### 3. Xem DAG Runs
```bash
docker-compose exec airflow-scheduler airflow dags list-runs -d tiki_crawl_products
```

### 4. Xem Tasks Trong DAG Run
```bash
docker-compose exec airflow-scheduler airflow tasks list tiki_crawl_products
```

## Các Lệnh Hữu Ích Khác

### Unpause DAG
```bash
docker-compose exec airflow-scheduler airflow dags unpause tiki_crawl_products
```

### Pause DAG
```bash
docker-compose exec airflow-scheduler airflow dags pause tiki_crawl_products
```

### Trigger DAG Thủ Công
```bash
docker-compose exec airflow-scheduler airflow dags trigger tiki_crawl_products
```

### Xem Variables
```bash
docker-compose exec airflow-scheduler airflow variables list
```

### Xem Variable Cụ Thể
```bash
docker-compose exec airflow-scheduler airflow variables get TIKI_DAG_SCHEDULE_MODE
```

## Xem Log Trong Airflow UI

1. Mở Airflow UI: http://localhost:8080
2. Vào DAG `tiki_crawl_products`
3. Click vào DAG Run gần nhất
4. Click vào task để xem log chi tiết

## Lưu Ý

- Các Variables có default values nên DAG vẫn chạy được ngay cả khi thiếu
- Log errors về missing variables không chặn DAG chạy, chỉ gây noise trong log
- DAG đang chạy ở chế độ manual, cần trigger thủ công hoặc set `TIKI_DAG_SCHEDULE_MODE=scheduled` để chạy tự động

