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

### 5. `demo_crawl_detail_async.py` ⭐
**Mục đích**: So sánh crawl product detail: Selenium vs AsyncHTTP (không dùng Selenium)

**Chức năng**:
- Crawl chi tiết sản phẩm bằng **AsyncHTTP** (fast, lightweight)
- Crawl chi tiết sản phẩm bằng **Selenium** (complete, JavaScript support)
- So sánh tốc độ, độ chính xác dữ liệu
- Tính toán performance metrics (speedup factor)
- Hiển thị detailed comparison results

**Ưu điểm AsyncHTTP**:
- ⚡ **Nhanh 5-10x** so với Selenium
- 💻 **Ít tài nguyên**: CPU, memory thấp hơn
- 🔄 **Dễ scale**: Crawl 100+ sản phẩm song song
- ✓ Lấy được 80-90% thông tin cần thiết

**Nhược điểm AsyncHTTP**:
- Không load JavaScript → thiếu một số dynamic content
- Sales_count có thể không đầy đủ
- Comments/reviews không lấy được (load qua AJAX)

**Cách chạy**:
```bash
python demos/demo_crawl_detail_async.py
```

**Output**:
```
data/test_output/demo_crawl_detail_comparison.json
```

### 6. `demo_crawl_detail_comparison.py` ⭐
**Mục đích**: Detailed benchmark - phân tích chi tiết Selenium vs AsyncHTTP

**Chức năng**:
- Benchmark chuyên sâu với nhiều metrics
- Đo lường data completeness score (0-100)
- So sánh success rate, avg time, data quality
- ASCII performance charts
- Smart recommendations dựa vào kết quả

---

## 🗂️ Archived Demos

Các demo sau đã được chuyển sang `demos/archive/` vì không còn sử dụng:
- `compare_three_methods.py` - Old comparison (superseded by newer demos)
- `COMPARISON_ANALYSIS.py` - Old analysis script
- `CRAWL_COMPARISON_GUIDE.md` - Old comparison guide
- `demo_all_crawl_methods_comprehensive.py` - Superseded by step-by-step demos
- `demo_all_methods.py` - Superseded by newer comparison demos
- `show_comparison_analysis.py` - Old analysis viewer
- `test_all_8_methods.py` - Old test script

**Metrics được đo lường**:
- ⏱️ Performance: total time, avg time, min/max time
- 📊 Data quality: completeness score (name, price, rating, images, specs)
- ✅ Success rate: crawl thành công % bao nhiêu
- 🎯 Data matching: so sánh dữ liệu giữa 2 cách crawl

**Recommendations**:
- ✓ "Use AsyncHTTP for bulk crawling (10-100+ products) - much faster and lighter"
- ✓ "Use Selenium for complete data - captures JavaScript-rendered content"
- ✓ "Use Hybrid approach - AsyncHTTP first, Selenium fallback for missing data"

**Cách chạy**:
```bash
python demos/demo_crawl_detail_comparison.py
```

**Output**:
```
data/test_output/demo_crawl_comparison_detailed.json

📊 BENCHMARK REPORT
====================================
🌐 SELENIUM
  success_count ...................... 3
  failure_count ...................... 0
  avg_time ........................... 45.32s
  avg_data_quality ................... 92.5/100

📡 ASYNC HTTP
  success_count ...................... 3
  failure_count ...................... 0
  avg_time ........................... 5.21s
  avg_data_quality ................... 85.0/100

💡 RECOMMENDATIONS
  best_for_speed ..................... AsyncHTTP
  speedup_factor ..................... 8.7x
  recommendation ..................... Use AsyncHTTP for bulk crawling...
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

### ⚡ Chạy benchmark crawl detail
**So sánh tốc độ & chất lượng dữ liệu: Selenium vs AsyncHTTP**

```bash
# Comparison cơ bản
python demos/demo_crawl_detail_async.py

# Benchmark chi tiết với recommendations
python demos/demo_crawl_detail_comparison.py
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

