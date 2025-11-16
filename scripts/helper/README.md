# Scripts Helper

## sync_test_dag.py

Script helper để tự động đồng bộ logic từ DAG chính (`tiki_crawl_products_dag.py`) sang test DAG (`tiki_crawl_products_test_dag.py`).

### Cách sử dụng

```bash
# Từ thư mục gốc của project
python scripts/sync_test_dag.py
```

### Chức năng

Script sẽ:
1. Đọc toàn bộ nội dung từ DAG chính
2. Tự động thay đổi các tham số cho test mode:
   - `dag_id`: `tiki_crawl_products` → `tiki_crawl_products_test`
   - `max_active_tasks`: `10` → `3`
   - `retries`: `3` → `1`
   - `max_products`: Variable → `10` (hardcode)
   - `max_pages`: `20` → `2`
   - `timeout`: `300` → `120`
   - `max_retries`: `3` → `2`
   - `execution_timeout`: Giảm xuống tối đa 5-10 phút
   - Thêm tag `"test"` vào tags
3. Ghi vào test DAG file

### Lợi ích

- ✅ Đảm bảo test DAG luôn đồng bộ với DAG chính
- ✅ Tự động áp dụng các thay đổi logic mới nhất
- ✅ Chỉ cần chạy một lệnh để cập nhật
- ✅ Giảm thiểu lỗi do copy-paste thủ công

### Lưu ý

- Script sẽ **ghi đè** file test DAG hiện tại
- Nên commit test DAG trước khi chạy script (để có thể rollback nếu cần)
- Kiểm tra kỹ test DAG sau khi chạy script

