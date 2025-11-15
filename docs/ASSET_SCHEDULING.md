# 📊 Asset-Aware Scheduling trong Tiki Data Pipeline

## Tổng quan

Dự án đã tích hợp **Asset-aware Scheduling** (Data-aware Scheduling) từ Airflow 2.7+ để quản lý dependencies dữ liệu một cách tự động và linh hoạt.

## 🎯 Assets/Datasets được định nghĩa

Pipeline sử dụng 4 datasets chính để track data flow:

| Dataset URI | Mô tả | Tạo bởi Task |
|:-----------:|:------|:-----------:|
| `tiki://products/raw` | Raw products từ crawl | `save_products` |
| `tiki://products/with_detail` | Products với chi tiết đầy đủ | `save_products_with_detail` |
| `tiki://products/transformed` | Products đã transform | `transform_products` |
| `tiki://products/final` | Products đã load vào database | `load_products` |

## 🔄 Data Flow với Assets

```
Crawl Products
    ↓ (tạo tiki://products/raw)
Crawl Details
    ↓ (tạo tiki://products/with_detail)
Transform Products
    ↓ (tạo tiki://products/transformed)
Load Products
    ↓ (tạo tiki://products/final)
Validate & Aggregate
```

## 📋 Cách sử dụng

### 1. Xem Assets trong Airflow UI

1. Truy cập Airflow UI: http://localhost:8080
2. Vào menu **Datasets** (hoặc **Assets**)
3. Xem danh sách các datasets và trạng thái cập nhật

### 2. Tạo DAG phụ thuộc vào Asset

Bạn có thể tạo DAG mới chạy khi asset được cập nhật:

```python
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import task

# Định nghĩa dataset
TRANSFORMED_PRODUCTS = Dataset("tiki://products/transformed")

# DAG sẽ chạy khi TRANSFORMED_PRODUCTS được cập nhật
with DAG(
    "tiki_analytics_dag",
    schedule=[TRANSFORMED_PRODUCTS],  # Data-aware scheduling!
    ...
) as dag:
    
    @task
    def analyze_products():
        # Phân tích products đã transform
        pass
    
    analyze_products()
```

### 3. Sử dụng Asset Aliases

Để tạo alias cho assets (hữu ích khi muốn thay đổi URI):

```python
from airflow.datasets import Dataset

# Tạo alias
PRODUCTS = Dataset("tiki://products/transformed")

# Sử dụng trong DAG
with DAG(
    "my_dag",
    schedule=[PRODUCTS],
    ...
):
    pass
```

### 4. Logic phức tạp với AND/OR

```python
from airflow.datasets import Dataset

RAW_PRODUCTS = Dataset("tiki://products/raw")
CATEGORIES = Dataset("tiki://categories")

# Chạy khi CẢ HAI assets được cập nhật (AND)
with DAG(
    "dag_and",
    schedule=[RAW_PRODUCTS & CATEGORIES],
    ...
):
    pass

# Chạy khi MỘT TRONG HAI assets được cập nhật (OR)
with DAG(
    "dag_or",
    schedule=[RAW_PRODUCTS | CATEGORIES],
    ...
):
    pass
```

## ⚙️ Cấu hình

### Enable/Disable Asset Scheduling

Trong Airflow Variables:
- `TIKI_USE_ASSET_SCHEDULING`: `true` hoặc `false` (mặc định: `false`)

### Kiểm tra Dataset Availability

Code tự động kiểm tra xem Airflow version có hỗ trợ Dataset không:

```python
try:
    from airflow.datasets import Dataset
    DATASET_AVAILABLE = True
except ImportError:
    DATASET_AVAILABLE = False
```

Nếu Airflow < 2.7, assets sẽ được set thành `None` và không ảnh hưởng đến DAG.

## 🔍 Monitoring Assets

### Xem Asset Updates

1. Vào **Datasets** trong Airflow UI
2. Click vào dataset để xem:
   - Lịch sử cập nhật
   - Các DAGs phụ thuộc
   - Timestamp cập nhật cuối

### Xem trong DAG Graph

Trong DAG graph view, bạn sẽ thấy:
- **Outlets** (mũi tên đi ra): Asset được tạo bởi task
- **Ins** (mũi tên đi vào): Asset mà task phụ thuộc vào

## 💡 Lợi ích

1. **Data-aware Scheduling**: DAG chạy khi dữ liệu sẵn sàng, không phụ thuộc thời gian
2. **Tách biệt DAGs**: Có thể tách thành nhiều DAGs độc lập
3. **Tự động hóa**: Transform tự động chạy khi có dữ liệu mới
4. **Linh hoạt**: Dễ dàng thêm DAGs mới phụ thuộc vào assets
5. **Tracking**: Dễ dàng track data lineage và dependencies

## 📝 Ví dụ: Tách DAG thành nhiều DAGs

Với Asset-aware scheduling, bạn có thể tách DAG hiện tại thành:

### DAG 1: Crawl
```python
with DAG("tiki_crawl", schedule="@daily") as dag:
    task_save_products = PythonOperator(
        ...,
        outlets=[RAW_PRODUCTS_DATASET]
    )
```

### DAG 2: Transform (chạy khi có raw products)
```python
with DAG("tiki_transform", schedule=[RAW_PRODUCTS_DATASET]) as dag:
    task_transform = PythonOperator(
        ...,
        ins=[RAW_PRODUCTS_DATASET],
        outlets=[TRANSFORMED_PRODUCTS_DATASET]
    )
```

### DAG 3: Load (chạy khi có transformed products)
```python
with DAG("tiki_load", schedule=[TRANSFORMED_PRODUCTS_DATASET]) as dag:
    task_load = PythonOperator(
        ...,
        ins=[TRANSFORMED_PRODUCTS_DATASET],
        outlets=[FINAL_PRODUCTS_DATASET]
    )
```

## 🚀 Best Practices

1. **Đặt tên Dataset rõ ràng**: Sử dụng URI format như `tiki://products/raw`
2. **Document Assets**: Ghi rõ asset nào được tạo bởi task nào
3. **Monitor Updates**: Theo dõi asset updates trong Airflow UI
4. **Test Dependencies**: Test các DAGs phụ thuộc vào assets
5. **Version Assets**: Có thể thêm version vào URI nếu cần: `tiki://products/v2/raw`

## 📚 Tài liệu tham khảo

- [Airflow Asset Scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html)
- [Airflow Datasets](https://airflow.apache.org/docs/apache-airflow/stable/concepts/datasets.html)

