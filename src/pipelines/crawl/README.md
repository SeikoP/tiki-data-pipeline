# Crawl Pipeline - Hướng Dẫn Sử Dụng

## 1. Crawl Danh Mục (Categories)

Tiki có cấu trúc danh mục phân cấp (5 level). Crawl danh mục **chia 3 bước**:

### Bước 1: Crawl Danh Mục Gốc (Level 0-1)
```bash
python src/pipelines/crawl/extract_category_link_selenium.py
```
- URL: `https://tiki.vn/nha-cua-doi-song/c1883`
- Output: `data/raw/categories.json` (các danh mục level 1 con)
- Thời gian: ~2 phút
- **Chỉ chạy 1 lần để khởi tạo**

### Bước 2A: Crawl Danh Mục Đệ Quy (Cách 1 - Chậm)
```bash
python src/pipelines/crawl/crawl_categories_recursive.py
```
- Output: `data/raw/categories_recursive.json`
- Thời gian: ~5-10 phút
- Phương pháp: Sequential (tuần tự)

### Bước 2B: Crawl Danh Mục Đệ Quy Tối Ưu (Cách 2 - Nhanh) ⭐
```bash
python src/pipelines/crawl/crawl_categories_optimized.py
```
- Output: `data/raw/categories_recursive_optimized.json`
- Thời gian: ~3-5 phút
- Phương pháp: Parallel với ThreadPoolExecutor
- **KHUYẾN NGHỊ: Dùng cách này**

### Thứ Tự Chạy Lần Đầu:
1. `extract_category_link_selenium.py` (lấy level 1)
2. `crawl_categories_optimized.py` (lấy hết tất cả level)

## 2. Crawl Chi Tiết Sản Phẩm (Product Details)

### Tự Động (DAG Airflow)
```bash
docker-compose up -d
# Truy cập: http://localhost:8080
# Trigger DAG: tiki_crawl_products
```

### Thủ Công (Testing)
```bash
python src/pipelines/crawl/crawl_products.py
```
- Crawl tất cả products từ categories.json
- Output: `data/raw/products.json`

```bash
python src/pipelines/crawl/crawl_products_detail.py
```
- Crawl chi tiết (price, rating, images, etc.) cho mỗi product
- Output: `data/raw/products_detail.json`

## 3. Cấu Trúc Category Path

**Tiki breadcrumb trên website chỉ hiển thị 3-4 level, nhưng Tiki có tối đa 5 level:**

```
Level 0: "Nhà cửa - đời sống" (Level cha - HIDDEN trên breadcrumb)
Level 1: "Ngoài trời & sân vườn" 
Level 2: "Áo mưa, ô dù và phụ kiện đi mưa"
Level 3: "Phụ kiện đi mưa"
Level 4: (nếu có subcategory)
```

**✅ VẤN ĐỀ ĐÃ ĐƯỢC SỬA:**

Trước: Breadcrumb từ website chỉ có 3 level, không có Level 0 (parent)

Giải pháp:
- Tạo `build_category_hierarchy.py` để mapping tất cả categories với parent chain
- Generate `category_hierarchy_map.json` chứa full hierarchy
- Sửa `extract_product_detail()` để auto-detect parent khi path có 3 level
- Tích hợp vào DAG để sử dụng hierarchy_map trong mỗi product extraction

**Kết quả:** Tất cả products đều có 4 levels với parent category tự động được thêm vào!

## 4. Quy Trình Crawl Hoàn Chỉnh (E2E)

```mermaid
extract_category_link_selenium.py 
    ↓
crawl_categories_optimized.py (tất cả danh mục với hierarchy)
    ↓
crawl_products.py (lấy product URLs từ categories)
    ↓
crawl_products_detail.py (lấy chi tiết + category_path)
    ↓
Transform (normalize dữ liệu)
    ↓
Load (đưa vào PostgreSQL)
    ↓
Warehouse (Star Schema)
```

## 5. Debugging

### Kiểm Tra Category Hierarchy
```bash
python -c "
import json
with open('data/raw/categories_recursive_optimized.json') as f:
    cats = json.load(f)
    print(f'Tổng: {len(cats)} danh mục')
    levels = {}
    for cat in cats:
        l = cat.get('level', 0)
        levels[l] = levels.get(l, 0) + 1
    for l in sorted(levels.keys()):
        print(f'  Level {l}: {levels[l]}')
"
```

### Kiểm Tra Product Category Path
```bash
python -c "
import psycopg2, json
conn = psycopg2.connect('dbname=crawl_data user=postgres password=postgres host=localhost')
cur = conn.cursor()
cur.execute('SELECT COUNT(*), jsonb_array_length(category_path) FROM products GROUP BY jsonb_array_length(category_path)')
for row in cur.fetchall():
    print(f'{row[1]} levels: {row[0]} products')
cur.close()
conn.close()
"
```

## 6. Thông Số Cấu Hình

| Tham Số | Giá Trị | Ghi Chú |
|---------|--------|--------|
| Root URL | `https://tiki.vn/nha-cua-doi-song/c1883` | Danh mục gốc |
| Max Level (Categories) | 3 | Độ sâu tối đa |
| Max Level (Category Path) | 4 | Tối đa 4 level trong product |
| Thread Pool Size | 3 | Số thread parallel |
| Cache Dir | `data/raw/cache/` | Cache crawl results |

## 7. Troubleshooting

| Lỗi | Nguyên Nhân | Giải Pháp |
|-----|-----------|----------|
| 465 products with 3 levels (CỐ ĐỊNH ✅) | Missing Level 0 (parent) | Dùng `build_category_hierarchy.py` + auto-detect trong `extract_product_detail` |
| Breadcrumb không tìm thấy | HTML không render | Dùng `__NEXT_DATA__` từ JSON thay vì HTML parsing |
| Category path có tên sản phẩm | Breadcrumb bị lộn | Filter product names từ breadcrumb |
| Driver Selenium error | Chrome chưa cài | Cài: `pip install webdriver-manager` |

## 8. Pipeline Initialization Checklist

Khi setup lần đầu:

- [ ] Chạy `extract_category_link_selenium.py` (lấy danh mục gốc)
- [ ] Chạy `crawl_categories_optimized.py` (lấy tất cả danh mục)
- [ ] Chạy `build_category_hierarchy.py` (tạo hierarchy map)
- [ ] Kiểm tra `data/raw/category_hierarchy_map.json` tồn tại
- [ ] Start Docker: `docker-compose up -d`
- [ ] Trigger DAG `tiki_crawl_products` trong Airflow UI
- [ ] Kiểm tra products trong DB có 4 levels

---

**Cập nhật lần cuối:** 2024-11-28

### Tính Năng Mới (v1.1)
- ✅ Auto-detect parent category cho products (3 → 4 levels)
- ✅ Category hierarchy mapping
- ✅ DAG integration với hierarchy_map caching
- ✅ Comprehensive README with troubleshooting
