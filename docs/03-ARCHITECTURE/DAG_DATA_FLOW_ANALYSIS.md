# Phân Tích Logic E2E và Data Flow của DAG

## 📊 Tổng Quan

Tài liệu này phân tích logic end-to-end (E2E) của DAG, chuẩn hoá luồng dữ liệu qua từng bước, và kiểm tra tính hợp lý của các đường dẫn data folder. Bổ sung thêm data contracts, XCom payloads, liên hệ với schema Warehouse, và các tham số cấu hình quan trọng.

## 🔍 Cấu Trúc Data Folder

```
data/
├── raw/                          # Dữ liệu thô từ crawl (Main DAG)
│   ├── categories.json
│   ├── categories_tree.json
│   ├── categories_recursive_optimized.json
│   └── products/
│       ├── cache/                # Cache products từ categories
│       ├── detail/
│       │   └── cache/            # Cache product details
│       ├── products.json         # Products sau khi merge
│       ├── products_with_detail.json  # Products với detail đầy đủ
│       └── crawl_progress.json   # Progress tracking
│
├── processed/                    # Dữ liệu đã transform (Main DAG)
│   ├── products_transformed.json
│   └── products_final.json
│
└── test_output/                  # Dữ liệu test (Test DAG)
    ├── products/
    │   ├── cache/
    │   ├── detail/
    │   │   └── cache/
    │   ├── products.json
    │   ├── products_with_detail.json
    │   └── crawl_progress.json
    └── processed/
        ├── products_transformed.json
        └── products_final.json
```

   ### Quy ước tên file và tính nguyên tử (atomic writes)
   - File hợp nhất (`products.json`, `products_with_detail.json`) chỉ ghi sau khi hợp lệ; khi ghi dùng mẫu atomic writer (ghi tạm rồi rename) để tránh file corrupt.
   - Cache luôn đặt dưới `cache/` tương ứng để phân biệt với output hợp nhất.
   - `crawl_progress.json` chứa offsets/batches để tiếp tục crawl an toàn.

## 🔄 Logic E2E Flow

### Main DAG (`tiki_crawl_products_dag.py`)

```
1. Load Categories (reference data)
   └─> Đọc: data/raw/categories_recursive_optimized.json
   └─> Ghi: Database (categories table)
   └─> XCom: `{ "categories_count": int, "sample": [str] }`

2. Crawl Categories (Dynamic Task Mapping)
   └─> Cache: data/raw/products/cache/{hash}.json
   └─> XCom: Danh sách sản phẩm dạng rút gọn `[ { product_id, name, url, category_url } ]` (giữ payload nhỏ, dữ liệu lớn ghi file)

3. Merge Products (fan-in)
   └─> Đọc: XCom từ crawl_category tasks
   └─> Ghi: data/raw/products/products.json
   └─> XCom: `{ "products_count": int, "output_path": str }`

4. Prepare Products for Detail (filter + plan)
   └─> Đọc: data/raw/products/products.json
   └─> Đọc: data/raw/products/crawl_progress.json (nếu có)
   └─> Đọc: data/raw/products/detail/cache/{product_id}.json (check cache)
   └─> Kiểm tra: Database (products có price và sales_count)
   └─> Output (XCom): List product_ids cần crawl detail (đã loại trùng + có cache hợp lệ sẽ skip)

5. Crawl Product Details (Dynamic Task Mapping)
   └─> Cache: data/raw/products/detail/cache/{product_id}.json
   └─> XCom: `{ product_id, detail_valid: bool }` (chi tiết đầy đủ ghi file, không nhét XCom)

6. Merge Product Details (fan-in)
   └─> Đọc: XCom từ crawl_product_detail tasks
   └─> Ghi: data/raw/products/products_with_detail.json
   └─> XCom: `{ "merged_count": int, "output_path": str }`

7. Transform Products (normalize + computed)
   └─> Đọc: data/raw/products/products_with_detail.json
   └─> Ghi: data/processed/products_transformed.json
   └─> XCom: `{ "transformed_count": int, "output_path": str }`

8. Load Products (DB upsert + final JSON)
   └─> Đọc: data/processed/products_transformed.json
   └─> Ghi: Database (products table)
   └─> Ghi: data/processed/products_final.json
   └─> XCom: `{ "loaded_count": int, "final_path": str }`

9. Validate Data (schema + duplicates + nulls)
   └─> Đọc: data/raw/products/products_with_detail.json
   └─> Validate: Schema, duplicates, null values
   └─> XCom: `{ "issues": [str], "summary": { ... } }`

10. Aggregate and Notify (report)
    └─> Đọc: data/raw/products/products_with_detail.json
    └─> Ghi: Summary report

### Ràng buộc & data contracts (tóm tắt)
- `category_path`: tối đa 5 cấp (`MAX_CATEGORY_LEVELS=5`), tránh tràn vào tên sản phẩm.
- Output chuẩn hoá theo `products_final.json` (xem guide chi tiết ở `docs/07-GUIDES/products_final_fields_vi.md`).
- XCom chỉ chứa metadata nhẹ và counters; dữ liệu lớn ghi vào file dưới `data/`.
```

### Test DAG (`tiki_crawl_products_test_dag.py`)

```
1. Load Categories
   └─> Đọc: data/raw/categories_recursive_optimized.json (CÙNG với Main DAG)
   └─> Ghi: Database (categories table) (CÙNG database)

2. Crawl Categories (Dynamic Task Mapping)
   └─> Cache: data/test_output/products/cache/{hash}.json
   └─> Output: XCom (products list)

3. Merge Products
   └─> Đọc: XCom từ crawl_category tasks
   └─> Ghi: data/test_output/products/products.json

4. Prepare Products for Detail
   └─> Đọc: data/test_output/products/products.json
   └─> Đọc: data/test_output/products/crawl_progress.json (nếu có)
   └─> Đọc: data/test_output/products/detail/cache/{product_id}.json (check cache)
   └─> Kiểm tra: Database (products có price và sales_count) (CÙNG database)
   └─> Output: List products cần crawl detail

5. Crawl Product Details (Dynamic Task Mapping)
   └─> Cache: data/test_output/products/detail/cache/{product_id}.json
   └─> Output: XCom (product detail)

6. Merge Product Details
   └─> Đọc: XCom từ crawl_product_detail tasks
   └─> Ghi: data/test_output/products/products_with_detail.json

7. Transform Products
   └─> Đọc: data/test_output/products/products_with_detail.json
   └─> Ghi: data/test_output/processed/products_transformed.json

8. Load Products
   └─> Đọc: data/test_output/processed/products_transformed.json
   └─> Ghi: Database (products table) (CÙNG database - ⚠️ CẢNH BÁO)
   └─> Ghi: data/test_output/processed/products_final.json

### Khác biệt chính giữa Main vs Test DAG
- Tham số crawl giảm: số products/page/timeout/slots/retries.
- Data folders tách biệt (`raw/processed` vs `test_output/*`).
- Database dùng chung (thiết kế có chủ đích) với upsert để tránh duplicate.

9. Validate Data
   └─> Đọc: data/test_output/products/products_with_detail.json
   └─> Validate: Schema, duplicates, null values

10. Aggregate and Notify
    └─> Đọc: data/test_output/products/products_with_detail.json
    └─> Ghi: Summary report
```

## ✅ Điểm Mạnh

1. **Tách biệt data folder**: Test DAG và Main DAG dùng folder riêng biệt
   - Main DAG: `data/raw/products/` và `data/processed/`
   - Test DAG: `data/test_output/products/` và `data/test_output/processed/`

2. **Cache riêng biệt**: Mỗi DAG có cache riêng, tránh conflict

3. **Progress tracking riêng**: Mỗi DAG có progress file riêng

4. **Logic nhất quán**: Cả 2 DAG đều follow cùng một flow logic

5. **Data contracts rõ ràng**: XCom nhẹ, file outputs có đường dẫn tiêu chuẩn, schema transform thống nhất.

## ⚠️ Vấn Đề Cần Lưu Ý

### 1. Database Sharing (✅ HỢP LÝ)

**Thiết kế**: Test DAG và Main DAG dùng **CÙNG MỘT DATABASE** - **ĐÂY LÀ THIẾT KẾ CÓ CHỦ ĐÍCH**.

```python
# Cả 2 DAG đều dùng:
db_name = Variable.get("POSTGRES_DB", default_var="crawl_data")
```

**Lý do thiết kế**:
- Test DAG là để **test với dữ liệu thực tế** nhưng với tham số giảm (ít products, ít pages, timeout ngắn hơn)
- Mục đích: **Theo dõi luồng E2E nhanh hơn** để verify logic pipeline hoạt động đúng
- Dữ liệu test cũng là dữ liệu thực tế, nên load vào cùng database là hợp lý
- Test DAG chỉ khác Main DAG ở:
  - Số lượng products crawl (10 vs không giới hạn)
  - Số pages crawl (2 vs 20)
  - Timeout ngắn hơn (2 phút vs 5 phút)
  - Max active tasks ít hơn (3 vs 10)
  - Retries ít hơn (1 vs 3)

**Lưu ý**:
- Test DAG vẫn crawl dữ liệu thực tế từ Tiki
- Dữ liệu được load vào cùng database với Main DAG
- Có thể có một số products trùng lặp nếu cả 2 DAG cùng crawl (nhưng có upsert logic để xử lý)
- Test DAG có logic kiểm tra database để tránh crawl lại products đã có (từ Main DAG hoặc từ lần test trước)

**Kết luận**: ✅ **Thiết kế hợp lý** - Test DAG là để test nhanh với dữ liệu thực tế, không phải test riêng biệt với dữ liệu giả.

### 2. Categories File Sharing

**Thiết kế**: Cả 2 DAG đều đọc từ cùng file categories:
```python
CATEGORIES_FILE = DATA_DIR / "raw" / "categories_recursive_optimized.json"
```

**Đánh giá**: ✅ **HỢP LÝ** - Categories là dữ liệu reference, không thay đổi thường xuyên, nên share là hợp lý. Cả 2 DAG đều cần cùng danh sách categories để crawl.

**Ràng buộc breadcrumb**:
- `category_path` được giới hạn 5 cấp (tham chiếu `MAX_CATEGORY_LEVELS=5`). Các task merge/transform sẽ truncate nếu vượt quá.

### 3. Kiểm Tra Database trong Prepare Products

**Thiết kế**: Test DAG kiểm tra database để tránh crawl lại products đã có.

**Đánh giá**: ✅ **HỢP LÝ** - Đây là tính năng hữu ích:
- Test DAG có thể skip products đã được crawl bởi Main DAG (hoặc từ lần test trước)
- Giúp test DAG chạy nhanh hơn vì không cần crawl lại products đã có
- Logic kiểm tra database đảm bảo chỉ skip products có detail đầy đủ (có price và sales_count)
- Nếu test DAG chạy trước Main DAG, nó sẽ crawl products mới và Main DAG sẽ skip những products đã có

**Kết luận**: ✅ **Thiết kế tốt** - Logic kiểm tra database giúp tránh crawl lại không cần thiết và tối ưu thời gian chạy.

### 4. Error handling & retry
- Crawl có cơ chế retry theo tham số DAG; lỗi tạm thời (HTTP, timeout) sẽ được retry giới hạn.
- Ghi file dùng atomic writer để tránh sinh file dở dang.
- Khi lỗi merge/transform, pipeline ghi log chi tiết và không làm hỏng file đã tồn tại.

## 📋 Checklist Logic E2E

### Main DAG

- [x] Load categories từ file
- [x] Crawl categories và cache
- [x] Merge products và lưu vào `raw/products/products.json`
- [x] Prepare products cho detail (check cache, progress, database)
- [x] Crawl product details và cache
- [x] Merge details và lưu vào `raw/products/products_with_detail.json`
- [x] Transform và lưu vào `processed/products_transformed.json`
- [x] Load vào database và lưu `processed/products_final.json`
- [x] Validate data
- [x] Aggregate và notify

### Validation bổ sung
- [x] `category_path` không vượt quá 5 cấp
- [x] `product_id` digits-only
- [x] `price <= original_price` nếu cả hai tồn tại

### Test DAG

- [x] Load categories từ file (cùng với Main DAG)
- [x] Crawl categories và cache vào `test_output/products/cache/`
- [x] Merge products và lưu vào `test_output/products/products.json`
- [x] Prepare products cho detail (check cache, progress, database)
- [x] Crawl product details và cache vào `test_output/products/detail/cache/`
- [x] Merge details và lưu vào `test_output/products/products_with_detail.json`
- [x] Transform và lưu vào `test_output/processed/products_transformed.json`
- [x] Load vào database (⚠️ CÙNG database với Main DAG)
- [x] Validate data
- [x] Aggregate và notify

## 🔧 Đề Xuất Cải Thiện (Tùy chọn)

### 1. Thêm Comment trong Code

Thêm comment rõ ràng trong test DAG để giải thích thiết kế:
```python
# Test DAG dùng cùng database với Main DAG vì:
# - Test với dữ liệu thực tế (không phải dữ liệu giả)
# - Mục đích: Test nhanh luồng E2E với tham số giảm
# - Dữ liệu test cũng là dữ liệu production hợp lệ
db_name = Variable.get("POSTGRES_DB", default_var="crawl_data")
```

### 2. Thêm Logging để Phân Biệt

Thêm logging để dễ phân biệt dữ liệu từ test DAG vs Main DAG:
```python
logger.info(f"🔬 TEST MODE: Loading {len(products)} products to database")
logger.info(f"   Source: Test DAG (reduced parameters for quick E2E testing)")
```

### 3. Thêm Metadata trong Database (Tùy chọn)

Nếu muốn track nguồn gốc dữ liệu, có thể thêm column `source_dag`:
```python
# Thêm vào schema
source_dag VARCHAR(50) DEFAULT 'tiki_crawl_products'

# Khi load từ test DAG
source_dag = 'tiki_crawl_products_test'
```

**Lưu ý**: Các đề xuất trên là tùy chọn, không bắt buộc vì thiết kế hiện tại đã hợp lý.

### 4. Thêm metric/performance tracking
- Ghi thêm thời lượng task, số lượng sản phẩm theo batch, cache hit-rate.
- Báo cáo tổng hợp: success/failure, avg crawl time, data completeness.

## 📝 Kết Luận

**Logic E2E**: ✅ **HỢP LÝ** - Flow logic rõ ràng, nhất quán giữa test và main DAG.

**Data Folder Structure**: ✅ **HỢP LÝ** - Tách biệt rõ ràng giữa test và production data để tránh conflict files.

**Database Sharing**: ✅ **HỢP LÝ** - Test DAG và Main DAG dùng cùng database là thiết kế có chủ đích:
- Test DAG test với **dữ liệu thực tế** nhưng với tham số giảm
- Mục đích: **Theo dõi luồng E2E nhanh hơn** để verify logic
- Dữ liệu test cũng là dữ liệu production hợp lệ
- Có logic upsert và kiểm tra database để tránh duplicate

**Categories Sharing**: ✅ **HỢP LÝ** - Share categories file là hợp lý vì là reference data.

**Tóm lại**: Thiết kế hiện tại **hoàn toàn hợp lý** cho mục đích test nhanh với dữ liệu thực tế. Test DAG không phải là test riêng biệt với dữ liệu giả, mà là test với dữ liệu thực tế nhưng với tham số giảm để chạy nhanh hơn.

**Liên hệ schema Warehouse**
- `products_final.json` → Load vào bảng `products` (upsert theo `product_id`).
- `category_path` → map sang `dim_category(level_1..level_5)` khi build Warehouse (truncate 5 cấp).
- Computed fields hỗ trợ báo cáo (revenue, savings, popularity, value).

## 🎯 Hành Động Tiếp Theo (Tùy chọn)

1. **Tùy chọn**: Thêm comment trong code để giải thích thiết kế database sharing
2. **Tùy chọn**: Thêm logging để phân biệt dữ liệu từ test DAG vs Main DAG
3. **Tùy chọn**: Thêm metadata `source_dag` trong database nếu muốn track nguồn gốc

**Lưu ý**: Các hành động trên là tùy chọn, không bắt buộc vì thiết kế hiện tại đã hợp lý và phù hợp với mục đích sử dụng.

