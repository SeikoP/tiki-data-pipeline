# 📚 Demo Files - Hướng dẫn sử dụng

Thư mục này chứa các file demo để chạy từng bước của pipeline hoặc chạy toàn bộ pipeline end-to-end.

## 📋 Các file demo

### 1. `demo_step1_crawl.py`
**Mục đích**: Crawl sản phẩm từ Tiki.vn

**Chức năng**:
- Crawl danh sách sản phẩm từ một danh mục
- Lưu kết quả vào `data/raw/products/demo_products.json`

**Cách chạy**:
```bash
python demos/demo_step1_crawl.py
```

### 2. `demo_step2_transform.py`
**Mục đích**: Transform dữ liệu sản phẩm

**Chức năng**:
- Đọc dữ liệu từ bước 1
- Normalize, validate, và tính computed fields
- Lưu kết quả vào `data/processed/demo_products_transformed.json`

**Cách chạy**:
```bash
python demos/demo_step2_transform.py
```

**Lưu ý**: Phải chạy `demo_step1_crawl.py` trước!

### 3. `demo_step3_load.py`
**Mục đích**: Load dữ liệu vào database

**Chức năng**:
- Đọc dữ liệu đã transform từ bước 2
- Load vào PostgreSQL database (nếu có)
- Lưu vào file JSON (backup)

**Cách chạy**:
```bash
python demos/demo_step3_load.py
```

**Lưu ý**: Phải chạy `demo_step2_transform.py` trước!

**Cấu hình database** (environment variables):
- `POSTGRES_HOST` (mặc định: `localhost`)
- `POSTGRES_PORT` (mặc định: `5432`)
- `POSTGRES_DB` (mặc định: `crawl_data`)
- `POSTGRES_USER` (mặc định: `airflow`)
- `POSTGRES_PASSWORD` (mặc định: `airflow`)

### 4. `demo_e2e_full.py`
**Mục đích**: Chạy toàn bộ pipeline từ đầu đến cuối

**Chức năng**:
- Chạy tất cả 3 bước liên tiếp: Crawl → Transform → Load
- Hiển thị thống kê cho từng bước

**Cách chạy**:
```bash
python demos/demo_e2e_full.py
```

## 🚀 Quick Start

### Chạy từng bước (khuyến nghị cho người mới)
```bash
# Bước 1: Crawl
python demos/demo_step1_crawl.py

# Bước 2: Transform
python demos/demo_step2_transform.py

# Bước 3: Load
python demos/demo_step3_load.py
```

### Chạy toàn bộ pipeline
```bash
python demos/demo_e2e_full.py
```

## 📁 Cấu trúc files output

Sau khi chạy các demo, bạn sẽ có các files sau:

```
data/
├── raw/
│   └── products/
│       └── demo_products.json              # Từ bước 1
└── processed/
    ├── demo_products_transformed.json      # Từ bước 2
    ├── demo_products_final.json            # Từ bước 3
    └── demo_e2e_products_final.json        # Từ demo_e2e_full.py
```

## ⚙️ Yêu cầu

1. **Dependencies**: Đã cài đặt `requirements.txt`
   ```bash
   pip install -r requirements.txt
   ```

2. **Database** (cho bước 3): PostgreSQL đang chạy (nếu muốn load vào DB)
   - Có thể bỏ qua nếu chỉ muốn lưu vào file JSON

## 🔍 Troubleshooting

### Lỗi import modules
```
❌ Lỗi import: No module named 'pipelines'
```
**Giải pháp**: Đảm bảo bạn đang chạy từ thư mục root của project

### Lỗi crawl
```
❌ Không crawl được sản phẩm nào!
```
**Giải pháp**: 
- Kiểm tra kết nối internet
- Tiki.vn có thể đã thay đổi cấu trúc HTML
- Thử lại sau vài phút

### Lỗi database connection
```
⚠️  Lỗi khi load vào database
```
**Giải pháp**:
- Kiểm tra PostgreSQL đang chạy
- Kiểm tra cấu hình database (environment variables)
- Dữ liệu vẫn được lưu vào file JSON nếu database không khả dụng

## 📝 Notes

- Các demo files sử dụng dữ liệu mẫu nhỏ để chạy nhanh
- Để crawl nhiều sản phẩm hơn, chỉnh sửa `max_pages` và `max_products` trong code
- Các file demo không ảnh hưởng đến dữ liệu production (dùng prefix `demo_`)

