# 🚀 Quick Start: Bật Asset Scheduling

## Bước 1: Kiểm tra Airflow Version

```bash
# Kiểm tra version
airflow version

# Yêu cầu: Airflow >= 2.7.0
# Nếu < 2.7, cần upgrade Airflow trước
```

## Bước 2: Chạy Setup Script

```bash
# Kiểm tra trạng thái hiện tại
python scripts/setup_asset_scheduling.py --check

# Bật Asset Scheduling
python scripts/setup_asset_scheduling.py --enable

# Verify
python scripts/setup_asset_scheduling.py --check
```

## Bước 3: Test Setup

```bash
# Chạy test script
python scripts/test_asset_scheduling.py
```

## Bước 4: Verify trong Airflow UI

1. **Restart Airflow** (nếu cần):
   ```bash
   # Nếu dùng Docker
   docker-compose restart airflow-scheduler
   docker-compose restart airflow-webserver
   ```

2. **Kiểm tra DAG**:
   - Vào http://localhost:8080
   - Tìm DAG `tiki_crawl_products`
   - Click vào DAG → Graph View
   - Kiểm tra tasks có **outlets** (mũi tên đi ra) không

3. **Kiểm tra Datasets**:
   - Vào menu **Datasets** (hoặc **Assets**)
   - Xem 4 datasets:
     - `tiki://products/raw`
     - `tiki://products/with_detail`
     - `tiki://products/transformed`
     - `tiki://products/final`

## Bước 5: Test End-to-End

1. **Trigger DAG**:
   ```bash
   airflow dags trigger tiki_crawl_products
   ```

2. **Monitor**:
   - Xem DAG chạy trong UI
   - Sau khi `save_products` xong, kiểm tra dataset `tiki://products/raw` được update
   - Sau khi `save_products_with_detail` xong, kiểm tra dataset `tiki://products/with_detail` được update

## Troubleshooting

### Lỗi: "Dataset class không có sẵn"
```bash
# Kiểm tra Airflow version
airflow version

# Nếu < 2.7, cần upgrade
pip install --upgrade apache-airflow
```

### Lỗi: "Variable không được set"
```bash
# Set thủ công trong Airflow UI
# Admin > Variables > Add
# Key: TIKI_USE_ASSET_SCHEDULING
# Value: true
```

### DAG không có outlets
- Kiểm tra Variable `TIKI_USE_ASSET_SCHEDULING = true`
- Restart Airflow scheduler
- Kiểm tra DAG parsing không có lỗi

## Next Steps

Sau khi Phase 1 hoàn thành, tiếp tục với:
- **Phase 2**: Tách DAG thành nhiều DAGs (xem [ASSET_IMPLEMENTATION_PLAN.md](./ASSET_IMPLEMENTATION_PLAN.md))
- **Phase 3**: Thêm DAGs mới (Analytics, Reporting)

