# Kế hoạch dọn dẹp dự án Tiki Data Pipeline

## 🗑️ FILES CẦN XÓA

### 1. Data Files - Trùng lặp/Không cần thiết
- [x] `data/raw/categories.json` - Chỉ có level 0, đã có trong categories_tree.json ✅ ĐÃ XÓA
- [x] `data/raw/categories_recursive_test.json` - File test, đã có categories_recursive_optimized.json ✅ ĐÃ XÓA
- [x] `data/raw/all_products_merged.json` - File cũ, đã có products_with_detail.json ✅ ĐÃ XÓA
- [x] `data/raw/products/products.json` - File trung gian, đã merge vào products_with_detail.json ✅ ĐÃ XÓA
- [x] `data/raw/products/products_batch_*.json` - File batch tạm thời ✅ ĐÃ XÓA
- [x] `data/raw/products/crawl_progress.json` - File progress tạm thời (nếu không crawl dở dang) ✅ ĐÃ XÓA

### 2. Source Code - File crawl cũ/không dùng
- [x] `src/pipelines/crawl/crawl_categories_optimized.py` - ĐÃ XÓA ✅
- [x] `src/pipelines/crawl/crawl_products.py` - ❌ ĐANG DÙNG TRONG DAG - GIỮ LẠI

### 3. Archive/Test Files - Đã hoàn thành
- [x] `verifydata/archive/*` - Các file verify/fix cũ (27 files) ✅ ĐÃ XÓA
- [x] `tests/archive/*` - Các test cũ (7 files) ✅ ĐÃ XÓA
- [x] `tests/__pycache__/*` - Python cache ✅ ĐÃ XÓA

---

## ✅ FILES CẦN GIỮ

### Data Files (Cần thiết)
- ✅ `data/raw/categories_recursive_optimized.json` - File categories chính cho DAG
- ✅ `data/raw/categories_tree.json` - Cấu trúc cây categories
- ✅ `data/raw/category_hierarchy_map.json` - Map hierarchy cho auto-parent detection
- ✅ `data/raw/products/products_with_detail.json` - File products cuối cùng
- ✅ `data/raw/products/cache/*` - Cache category products
- ✅ `data/raw/products/detail/cache/*` - Cache product details

### Source Code (Đang dùng)
- ✅ `src/pipelines/crawl/crawl_categories_recursive.py` - File crawl categories chính (đã cải tiến)
- ✅ `src/pipelines/crawl/extract_category_link_selenium.py` - Module parse categories
- ✅ `src/pipelines/crawl/crawl_products_detail.py` - Crawl product details
- ✅ `airflow/dags/tiki_crawl_products_dag.py` - DAG chính

---

## 🔧 DAG TASKS CẦN TỐI ƯU

### Tasks có thể xóa/merge:
1. **`extract_and_load_categories_to_db`** - Nếu không cần load categories vào DB trước khi crawl
2. **Batch processing tasks** - Nếu không cần chia batch (có thể merge trực tiếp)
3. **Progress tracking** - Nếu không cần multi-day crawling

### Tasks cần giữ:
1. ✅ `load_categories` - Load danh sách categories
2. ✅ `crawl_category_products` - Crawl products từ categories
3. ✅ `merge_and_save_products` - Merge products
4. ✅ `crawl_product_details` - Crawl chi tiết sản phẩm
5. ✅ `save_products_with_detail` - Lưu products có detail
6. ✅ `transform_and_load` - Transform và load vào DB
7. ✅ `validate_data` - Validate dữ liệu
8. ✅ `aggregate_and_notify` - Tổng hợp và thông báo

---

## 📋 EXECUTION PLAN

### Phase 1: Xóa files không cần thiết
```bash
# Data files
rm data/raw/categories.json
rm data/raw/categories_recursive_test.json
rm data/raw/all_products_merged.json
rm data/raw/products/products.json
rm data/raw/products/products_batch_*.json
rm data/raw/products/crawl_progress.json

# Archive files
rm -rf verifydata/archive
rm -rf tests/archive
rm -rf tests/__pycache__
```

### Phase 2: Tối ưu DAG (cần review code)
- [x] Xem xét loại bỏ batch processing nếu không cần -> ✅ GIỮ LẠI (Cần thiết cho Driver Pooling/Parallel)
- [x] Xem xét loại bỏ progress tracking nếu không multi-day crawl -> ✅ GIỮ LẠI (Cần thiết cho Large Data Crawling)
- [x] Merge các tasks nhỏ thành tasks lớn hơn -> ✅ ĐÃ REVIEW (Structure hiện tại đã tối ưu với TaskGroup)

### Phase 3: Cleanup cache (tùy chọn)
- [x] Giữ cache nếu muốn tránh crawl lại -> ❌ KHÔNG GIỮ
- [x] Xóa cache nếu muốn crawl fresh data -> ✅ ĐÃ XÓA (Fresh start)

---

## ⚠️ LƯU Ý

1. **Backup trước khi xóa** - Tạo backup của data/ và src/ trước
2. **Test DAG** - Test DAG sau khi xóa files
3. **Kiểm tra dependencies** - Đảm bảo không có file nào đang được dùng
4. **Git commit** - Commit changes từng phase để dễ rollback
