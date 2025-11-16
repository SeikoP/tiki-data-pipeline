# Asset Scheduling Setup Scripts

## Scripts có sẵn

### 1. `setup_asset_scheduling.py`
Script chính để setup Asset Scheduling.

**Usage:**
```bash
# Kiểm tra trạng thái
python scripts/setup_asset_scheduling.py --check

# Bật Asset Scheduling
python scripts/setup_asset_scheduling.py --enable

# Tắt Asset Scheduling
python scripts/setup_asset_scheduling.py --disable

# Liệt kê datasets
python scripts/setup_asset_scheduling.py --list-datasets
```

**Lưu ý:** Script này cần chạy trong môi trường có Airflow. Nếu không có Airflow, sử dụng Airflow CLI trực tiếp:

```bash
# Kiểm tra version
airflow version

# Set Variable
airflow variables set TIKI_USE_ASSET_SCHEDULING true

# Get Variable
airflow variables get TIKI_USE_ASSET_SCHEDULING
```

### 2. `test_asset_scheduling.py`
Script để test Asset Scheduling setup.

**Usage:**
```bash
python scripts/test_asset_scheduling.py
```

**Test gì:**
- Variable `TIKI_USE_ASSET_SCHEDULING`
- Asset definitions trong `dag_assets`
- DAG parsing với Asset

## Quick Start (không có Airflow trong môi trường)

Nếu không có Airflow trong môi trường Python hiện tại, sử dụng Airflow CLI:

```bash
# 1. Kiểm tra Airflow version
airflow version

# 2. Bật Asset Scheduling
airflow variables set TIKI_USE_ASSET_SCHEDULING true

# 3. Verify
airflow variables get TIKI_USE_ASSET_SCHEDULING

# 4. Test DAG parsing
airflow dags list | grep tiki_crawl_products

# 5. Trigger DAG
airflow dags trigger tiki_crawl_products
```

## Verify trong Airflow UI

1. Vào http://localhost:8080
2. Menu **Datasets** → Xem 4 datasets
3. DAG `tiki_crawl_products` → Graph View → Xem tasks có outlets

