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

### 4. XCom Warnings - No XCom value found

**Vấn đề:** Các warnings xuất hiện khi task `merge_product_details` cố gắng lấy XCom từ các task `crawl_product_detail` không có value:

```
WARNING - No XCom value found; defaulting to None. 
key=return_value 
dag_id=tiki_crawl_products 
task_id=crawl_product_details.crawl_product_detail 
run_id=manual__2025-11-16T09:09:56+00:00 
map_index=144
```

**Nguyên nhân:**
1. **Task timeout:** Task `crawl_product_detail` có `execution_timeout=15 phút`. Nếu task chạy quá lâu, Airflow sẽ kill task trước khi return XCom value.
2. **Task fail sớm:** Task có thể fail ngay từ đầu (trước khi vào function) do lỗi import, memory error, hoặc lỗi khác.
3. **Exception không được catch:** Một số exception có thể không được catch đúng cách, khiến task fail mà không return value.

**Giải pháp:**

**1. Code đã xử lý:**
- Task `merge_product_details` đã có xử lý để bỏ qua các map_index không có XCom (try-except)
- Function `crawl_single_product_detail` đã được cải thiện để đảm bảo luôn return result hợp lệ

**2. Kiểm tra logs của các task bị thiếu XCom:**
```bash
# Xem log của task cụ thể với map_index
docker-compose exec airflow-scheduler airflow tasks logs \
  tiki_crawl_products \
  crawl_product_details.crawl_product_detail \
  <run_id> \
  --map-index 144
```

**3. Nếu nhiều tasks bị timeout:**
- Tăng `execution_timeout` cho task `crawl_product_detail` (hiện tại: 15 phút)
- Giảm số lượng products crawl cùng lúc (chia nhỏ batch)
- Tối ưu code crawl để chạy nhanh hơn

**4. Nếu tasks fail do lỗi:**
- Xem log của từng task để biết lỗi cụ thể
- Kiểm tra Redis connection (nếu dùng cache)
- Kiểm tra Selenium/Chrome driver (nếu dùng Selenium)
- Kiểm tra memory usage (có thể cần tăng memory limit)

**5. Suppress warnings (nếu đã xử lý):**
Warnings này không ảnh hưởng đến kết quả vì `merge_product_details` đã xử lý để bỏ qua. Có thể suppress bằng cách:
- Cấu hình Airflow logging level
- Hoặc bỏ qua warnings này vì đã được xử lý trong code

**Kiểm tra task sau `crawl_product_detail`:**

**Task `merge_product_details`:**
- Đã xử lý để bỏ qua các map_index không có XCom
- Log sẽ hiển thị số lượng results thực tế lấy được
- Nếu thiếu > 20%, sẽ có warning và thử lấy từng map_index riêng lẻ

**Task `save_products_with_detail`:**
- Lấy kết quả từ `merge_product_details` qua XCom
- Nếu không có XCom, sẽ fallback về file output
- Task này sẽ chạy thành công ngay cả khi một số products không có detail

**Task `transform_products`:**
- Lấy file từ `save_products_with_detail`
- Transform tất cả products có trong file
- Products không có detail sẽ được transform với dữ liệu cơ bản

**Task `load_to_database`:**
- Load tất cả products đã transform vào database
- Products không có detail vẫn được load (với detail = NULL)

## Lưu Ý

- Các Variables có default values nên DAG vẫn chạy được ngay cả khi thiếu
- Log errors về missing variables không chặn DAG chạy, chỉ gây noise trong log
- DAG đang chạy ở chế độ manual, cần trigger thủ công hoặc set `TIKI_DAG_SCHEDULE_MODE=scheduled` để chạy tự động
- **XCom warnings không ảnh hưởng đến kết quả:** Code đã xử lý để bỏ qua các tasks không có XCom value. Warnings chỉ để thông báo, không chặn pipeline chạy.

