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

## 🖼️ Xuất Hình DAG (Không dùng code minh họa)

### Phương pháp 1: Screenshot trực tiếp từ Airflow UI (Graph View)
- Mở Airflow Web UI và vào DAG cần chụp.
- Chọn Graph View, bật/ẩn TaskGroup tuỳ nhu cầu để khung hình rõ ràng.
- Phóng to/thu nhỏ bằng điều khiển zoom trên UI để vừa khung.
- Chụp màn hình bằng công cụ hệ điều hành (Windows: Snipping Tool/Snipping Bar) và lưu PNG.
- Ưu điểm: Nhanh, đúng trạng thái thực tế của DAG (bao gồm màu trạng thái, nhóm, nhãn).
- Hạn chế: Chất lượng phụ thuộc độ phân giải màn hình; không tự động cập nhật khi DAG thay đổi.

### Phương pháp 2: Render bằng Graphviz (xuất file ảnh từ cấu trúc DAG)
- Yêu cầu: Graphviz đã cài trong hệ thống và nằm trong `PATH`; Airflow có thể gọi Graphviz để tạo hình DAG.
- Cách thực hiện (không cần Python code minh hoạ):
   - Đảm bảo môi trường chạy cùng nơi chứa DAG (container Airflow hoặc máy dev có thể import DAG).
   - Sử dụng cơ chế render của DAG để tạo ra tệp hình (PNG/SVG) dựa trên sơ đồ phụ thuộc task.
   - Chỉ định tên DAG (`dag_id`) và đường dẫn đầu ra cho tệp ảnh.
- Lưu ý cấu hình:
   - Cài đặt Graphviz trên Windows: tải bản cài đặt chính thức (Graphviz MSI), sau đó thêm thư mục `bin` của Graphviz vào biến môi trường `PATH`.
   - Trong Docker compose stack của dự án, Graphviz không được cài sẵn; nếu cần render trong container, bổ sung cài Graphviz vào image hoặc chạy thao tác render từ môi trường máy chủ có Graphviz.
   - Khi DAG phức tạp (nhiều TaskGroup, Dynamic Task Mapping), hình ảnh có thể rất lớn; cân nhắc xuất định dạng SVG để giữ độ nét khi phóng to.
   - Trước khi render, bảo đảm DAG không có chu trình (cycle) để tránh lỗi khi tạo đồ thị.

### Khuyến nghị chất lượng hình
- Sử dụng nền trắng, độ phân giải cao (2K/4K) cho ảnh PNG khi chụp từ UI.
- Với ảnh render, chọn định dạng SVG cho tài liệu kỹ thuật để đảm bảo nét khi phóng to; dùng PNG cho báo cáo/slide.
- Ẩn bớt các nhánh ít liên quan (collapse TaskGroup) để tăng khả năng đọc.
- Thêm chú thích (legend) về nhóm nhiệm vụ chính: Crawl, Merge, Transform, Load, Validate.

### Vị trí lưu trữ ảnh trong repo
- Đề xuất lưu các ảnh DAG dưới `docs/03-ARCHITECTURE/assets/` với quy ước tên: `dag_{dag_id}_{yyyy-mm-dd}.png` hoặc `.svg`.
- Cập nhật tham chiếu trong tài liệu này để trỏ tới ảnh mới khi cấu trúc DAG thay đổi.

## 🎯 Hành Động Tiếp Theo (Tùy chọn)

1. **Tùy chọn**: Thêm comment trong code để giải thích thiết kế database sharing
2. **Tùy chọn**: Thêm logging để phân biệt dữ liệu từ test DAG vs Main DAG
3. **Tùy chọn**: Thêm metadata `source_dag` trong database nếu muốn track nguồn gốc

**Lưu ý**: Các hành động trên là tùy chọn, không bắt buộc vì thiết kế hiện tại đã hợp lý và phù hợp với mục đích sử dụng.

'''
## 🔗 Chi Tiết Nodes và Tasks Con (DAG Structure)

Phần này mô tả từng **node** và các **tasks con** mà nó chứa, kèm quy trình và mục đích cụ thể. Mỗi task được thực hiện bởi một operator (PythonOperator, TaskGroup, vv.) trong `tiki_crawl_products_dag.py`.



### **Node 1: Load and Prepare**

Nhóm tasks để nạp dữ liệu tham chiếu và chuẩn bị cho crawl.

#### **Task 1.1: Load Categories**
- **Quy trình:**
  - Đọc file `data/raw/categories_recursive_optimized.json`
  - Parse JSON, validate structure
  - Insert/upsert vào bảng `categories` trong DB
  - Ghi XCom: danh sách categories count + sample
- **Mục đích:** Cung cấp danh sách danh mục tham chiếu (reference data) để các task crawl dùng trong bước tiếp theo
- **Operator:** `PythonOperator` với function `load_categories(**context)` (dòng ~830)
- **Input:** `data/raw/categories_recursive_optimized.json`
- **Output:** Bảng `categories` (upsert); XCom: `{ categories_count: int, sample: [str] }`
- **Error handling:** Try-catch; log exception; raise nếu file không tìm thấy

---

### **Node 2: Crawl Categories (Listings)**

Nhóm tasks để crawl danh sách sản phẩm từ mỗi danh mục (Dynamic Task Mapping).

#### **Task 2.1: Crawl Single Category [Dynamic per category_id]**
- **Quy trình:**
  - Nhận category dict từ expand (category_id, name, url, is_leaf)
  - Gửi request tới Tiki API với pagination (page 1, 2, ...)
  - Parse response HTML, extract product info (id, name, price, url, rating)
  - Lưu cache per-category vào `data/raw/products/cache/{hash}.json`
  - Ghi XCom: danh sách products (id, name, url, category_url)
- **Mục đích:** Thu thập danh sách sản phẩm ban đầu từ mỗi leaf category, tối ưu bằng cache
- **Operator:** `PythonOperator` (Dynamic Task Mapping) với function `crawl_single_category(category={category}, **context)` (dòng ~872)
- **Input:** Category dict (từ node 1 expand)
- **Output:** Cache file; XCom: `[{ product_id, name, url, category_url }, ...]`
- **Cấu hình:** `tiki_max_products_per_category` (Airflow Variables), timeout, retries
- **Error handling:** Try-catch HTTP errors, timeout; ghi Dead Letter Queue; tiếp tục category tiếp theo
- **Dependencies:** Phụ thuộc Task 1.1 (`load_categories`)
- **Số tasks:** Động — 1 task per leaf category (~50-100 tasks)

---

### **Node 3: Merge and Deduplicate**

Nhóm tasks để hợp nhất kết quả crawl từ các danh mục.

#### **Task 3.1: Merge Products**
- **Quy trình:**
  - Lấy XCom từ tất cả tasks trong Node 2 (crawl_single_category[*])
  - Iterate mỗi task result, extract danh sách products
  - Deduplicate theo `product_id` (dùng Python dict)
  - Validate schema cơ bản (id, name, url tồn tại)
  - Ghi file `data/raw/products/products.json` bằng atomic writer
  - Ghi XCom: products_count, output_path
- **Mục đích:** Gộp products từ tất cả categories thành một danh sách duy nhất, loại trùng, chuẩn bị cho bước crawl detail
- **Operator:** `PythonOperator` với function `merge_products(**context)` (dòng ~1097)
- **Input:** XCom từ Node 2 tasks
- **Output:** `data/raw/products/products.json`; XCom: `{ products_count: int, output_path: str }`
- **Error handling:** Try-catch; log hàng lỗi; rollback nếu merge fail
- **Dependencies:** Phụ thuộc tất cả tasks trong Node 2

---

### **Node 4: Prepare Details Crawl**

Nhóm tasks để chuẩn bị danh sách products cần crawl detail.

#### **Task 4.1: Prepare Products for Detail Crawling**
- **Quy trình:**
  - Đọc file `data/raw/products/products.json` từ Node 3
  - Đọc `data/raw/products/crawl_progress.json` (nếu tồn tại) để lấy offset
  - Kiểm tra cache detail: đọc `data/raw/products/detail/cache/{product_id}.json`
  - Kiểm tra DB: query bảng `products` để tìm records có `price` + `sales_count` (chứng tỏ detail đã crawl)
  - Chia products cần crawl thành batches (batch size ~10 products)
  - Ghi XCom: danh sách batches (array of arrays)
- **Mục đích:** Tạo kế hoạch crawl detail: skip products đã có cache/DB hợp lệ; chia thành batches để tối ưu parallel execution
- **Operator:** `PythonOperator` với function `prepare_products_for_detail(**context)` (dòng ~1434)
- **Input:** `data/raw/products/products.json`, cache files, DB query
- **Output:** XCom: `[[ product_batch_0 ], [ product_batch_1 ], ...]`; File progress update
- **Cấu hình:** TTL cache (7 days), batch size, max products per run
- **Error handling:** Try-catch; fallback: crawl toàn bộ nếu cache/progress corrupt
- **Dependencies:** Phụ thuộc Task 3.1 (`merge_products`)

### Node 3: `merge_products` (PythonOperator)
- **Function:** `merge_products(**context)` (dòng ~1097)
- **Loại:** Fan-in task; gộp kết quả từ tất cả tasks trong node 2
- **Mục đích:** Hợp nhất products từ tất cả categories, loại trùng
- **Input:** XCom từ `crawl_single_category[*]` tasks
- **Output:** `data/raw/products/products.json`; XCom: `{ products_count, output_path }`
- **Deduplication:** Theo `product_id` (Python dict với product_id là key)
- **Error handling:** Try-catch; rollback nếu merge lỗi
- **Dependencies:** Phụ thuộc node 2 (tất cả tasks trong TaskGroup phải hoàn thành)

### Node 4: `prepare_products_for_detail` (PythonOperator)
- **Function:** `prepare_products_for_detail(**context)` (dòng ~1434)
- **Loại:** Preparation/Planning task (không crawl, chỉ plan)
- **Mục đích:** Tạo danh sách products cần crawl detail; skip cache hợp lệ; track progress
- **Input:** `products.json` từ node 3; check cache, DB, progress file
- **Output:** XCom: danh sách `product_id` cần crawl (chia thành batches)
- **Logic:** Skip nếu cache hợp lệ + TTL chưa hết; kiểm DB để tránh crawl lại
- **Cấu hình:** TTL cache (7 ngày), batch size, progress file
- **Error handling:** Try-catch; fallback: crawl toàn bộ nếu cache/progress lỗi
- **Dependencies:** Phụ thuộc node 3 (`merge_products`)

---

### **Node 5: Crawl Product Details (Listings)**

Nhóm tasks để crawl thông tin chi tiết sản phẩm từ mỗi batch (Dynamic Task Mapping).

#### **Task 5.1: Crawl Product Batch [Dynamic per batch_index]**
- **Quy trình:**
  - Nhận batch danh sách products từ expand (mỗi batch ~10 products)
  - Mở Selenium WebDriver (pool reusable drivers nếu có sẵn)
  - Với mỗi product trong batch: gửi request tới product page, extract brand/specs/images/breadcrumbs
  - Lưu cache per-product vào `data/raw/products/detail/cache/{product_id}.json`
  - Ghi XCom: danh sách `{ product_id, detail_valid: bool }`
  - Đóng driver hoặc trả về pool
- **Mục đích:** Thu thập thông tin chi tiết sản phẩm (brand, specs, images, breadcrumbs) từ product page, tối ưu bằng driver pooling và batch processing
- **Operator:** `PythonOperator` (Dynamic Task Mapping) với function `crawl_product_batch(product_batch={batch}, batch_index={idx}, **context)` (dòng ~1795)
- **Input:** Batch dict (danh sách products từ node 4 expand)
- **Output:** Cache files; XCom: `[{ product_id, detail_valid }, ...]`
- **Cấu hình:** Batch size (~10 products), driver timeout, retries, max concurrent drivers
- **Error handling:** Try-catch per-product; retry exponential backoff; skip product nếu fail > 3 lần; ghi Dead Letter Queue
- **Dependencies:** Phụ thuộc Task 4.1 (`prepare_products_for_detail`)
- **Số tasks:** Động — 1 task per batch (~10-50 tasks tuỳ batch size)

---

### **Node 6: Merge and Save Details**

Nhóm tasks để hợp nhất chi tiết sản phẩm vào danh sách chính.

#### **Task 6.1: Merge Product Details**
- **Quy trình:**
  - Lấy XCom từ tất cả tasks trong Node 5 (crawl_product_batch[*])
  - Đọc file `data/raw/products/products.json` từ Node 3
  - Với mỗi product: đọc cache detail `data/raw/products/detail/cache/{product_id}.json`
  - Merge chi tiết (brand, specs, images, breadcrumbs) vào product dict
  - Validate schema (bắt buộc brand, specs; breadcrumbs ≤5 levels)
  - Ghi XCom: merged_count, detail_valid_rate
- **Mục đích:** Gộp product details từ tất cả batches vào products list, đảm bảo tính đầy đủ
- **Operator:** `PythonOperator` với function `merge_product_details(**context)` (dòng ~2791)
- **Input:** XCom từ Node 5 tasks; cache detail files
- **Output:** XCom: `{ merged_count: int, detail_valid_rate: float }`
- **Error handling:** Try-catch; validate schema; skip products lỗi; log exceptions
- **Dependencies:** Phụ thuộc tất cả tasks trong Node 5

#### **Task 6.2: Save Products with Detail (Atomic Write)**
- **Quy trình:**
  - Ghi merged products list bằng atomic writer
  - Ghi temp file → rename để ensure atomicity
  - Validate output file integrity (file size > 0, valid JSON)
  - Ghi XCom: output_path, file_size
- **Mục đích:** Lưu products_with_detail.json bằng atomic writer để tránh corrupt file nếu crash
- **Operator:** `PythonOperator` với function `save_products_with_detail(**context)` (dòng ~3482)
- **Input:** Merged products từ Task 6.1
- **Output:** `data/raw/products/products_with_detail.json`; XCom: `{ output_path: str, file_size: int }`
- **Error handling:** Try-catch; rollback nếu rename fail; validate JSON trước lưu
- **Dependencies:** Phụ thuộc Task 6.1 (`merge_product_details`)

---

### **Node 7: Transform and Normalize**

Nhóm tasks để normalize, validate, và compute derived fields.

#### **Task 7.1: Transform Products**
- **Quy trình:**
  - Đọc file `data/raw/products/products_with_detail.json`
  - Parse JSON, iterate mỗi product
  - Type conversion: str → int/float (price, sales_count, rating)
  - Business rules validation: price ≤ original_price, rating ∈ [0,5], product_id digits-only
  - Compute fields: discount_percent = (original_price - price) / original_price * 100; estimated_revenue = price * sales_count; popularity_score = sales_count / max_sales; value_score = (discount_percent + popularity_score) / 2
  - Truncate category_path nếu > 5 cấp
  - Ghi output file, XCom: transformed_count, validation_errors
- **Mục đích:** Normalize dữ liệu (type conversion, validation), compute derived metrics (discount_percent, revenue, scores)
- **Operator:** `PythonOperator` với function `transform_products(**context)` (dòng ~3554)
- **Input:** `data/raw/products/products_with_detail.json`
- **Output:** `data/processed/products_transformed.json`; XCom: `{ transformed_count: int, validation_errors: [str] }`
- **Cấu hình:** Validation rules, compute formulas, category_path max level
- **Error handling:** Try-catch; log hàng lỗi; skip và continue (không fail toàn DAG)
- **Dependencies:** Phụ thuộc Task 6.2 (`save_products_with_detail`)

---

### **Node 8: Load to Database**

Nhóm tasks để upsert sản phẩm vào PostgreSQL.

#### **Task 8.1: Load Products**
- **Quy trình:**
  - Đọc file `data/processed/products_transformed.json`
  - Chia thành batches (batch size 100-1000 rows)
  - Với mỗi batch: prepare INSERT/UPDATE SQL `ON CONFLICT (product_id) DO UPDATE SET ...`
  - Execute batch transaction
  - Handle constraint violations (log + skip nếu cần)
  - Ghi final JSON output, XCom: loaded_count, upsert_stats
- **Mục đích:** Upsert products vào DB `crawl_data.products`; idempotent (run lại không tạo duplicate)
- **Operator:** `PythonOperator` với function `load_products(**context)` (dòng ~3915)
- **Input:** `data/processed/products_transformed.json`
- **Output:** Bảng `products` (upsert); `data/processed/products_final.json`; XCom: `{ loaded_count: int, upsert_stats: {...} }`
- **Cấu hình:** Batch size (default 500), connection string, retry policy
- **Error handling:** Try-catch; rollback transaction nếu constraint violation; ghi Dead Letter Queue
- **Dependencies:** Phụ thuộc Task 7.1 (`transform_products`)

---

### **Node 9: Validate Data Quality**

Nhóm tasks để kiểm tra chất lượng dữ liệu.

#### **Task 9.1: Validate Data**
- **Quy trình:**
  - Đọc file `data/raw/products/products_with_detail.json`
  - Query DB snapshot (tối đa 1000 records)
  - Kiểm tra schema: fields bắt buộc tồn tại
  - Kiểm tra duplicates: không có duplicate `product_id`
  - Kiểm tra nulls: required fields không null
  - Kiểm tra integrity: category_path ≤ 5 cấp, price ≥ 0, rating ∈ [0,5]
  - Ghi validation report, XCom: issues, summary stats
  - Fail DAG nếu validation_rate < 95%
- **Mục đích:** Kiểm tra schema, duplicates, nulls, và category_path integrity; detect lỗi dữ liệu sớm
- **Operator:** `PythonOperator` với function `validate_data(**context)` (dòng ~4175)
- **Input:** `data/raw/products/products_with_detail.json`; DB query results
- **Output:** Validation report (file + XCom); XCom: `{ issues: [str], validation_rate: float, summary: {...} }`
- **Cấu hình:** Min validation rate threshold (default 95%), sample size
- **Error handling:** Try-catch; log chi tiết; alert nếu issues; fail DAG nếu rate < threshold
- **Dependencies:** Có thể chạy parallel với Task 8.1 hoặc sau (tuỳ DAG setup); thường sau Task 6.2 để validate data trước load

---

### **Node 10: Aggregate and Notify**

Nhóm tasks để tổng hợp chỉ số và gửi thông báo.

#### **Task 10.1: Aggregate and Notify**
- **Quy trình:**
  - Đọc file `data/raw/products/products_with_detail.json`
  - Aggregate: tổng products, categories, avg rating, avg discount_percent
  - Calculate: estimated_revenue (sum price × sales_count), cache_hit_rate, validation_stats
  - Format message: "✅ ETL Success: {products_count} products, ${revenue}M revenue, {validation_rate}% data quality"
  - Call AISummarizer (nếu có) để tạo AI summary text
  - Gửi Discord message qua DiscordNotifier (webhook)
  - Ghi summary report file, XCom: message_sent: bool
- **Mục đích:** Tổng hợp chỉ số (counts, revenue, quality) và gửi thông báo qua Discord; provide ops visibility
- **Operator:** `PythonOperator` với function `aggregate_and_notify(**context)` (dòng ~4299)
- **Input:** `data/raw/products/products_with_detail.json`; XCom từ các tasks
- **Output:** Discord notification; summary report file; XCom: `{ message_sent: bool, summary_stats: {...} }`
- **Cấu hình:** Discord webhook URL (Airflow Variables), AI summarizer settings
- **Error handling:** Try-catch; retry gửi (3 retries); fallback lưu file nếu Discord down
- **Dependencies:** Phụ thuộc Task 9.1 (`validate_data`) hoặc Task 8.1 (tuỳ DAG setup)

---

### **Node 11: Maintenance Tasks (Optional)**

Nhóm tasks tuỳ chọn để health check, backup, cleanup.

#### **Task 11.1: Health Check Monitoring**
- **Quy trình:**
  - Ping Tiki API endpoint (check nếu API accessible)
  - Query DB (check connection + select count(*))
  - Ping Redis (check connection + ping)
  - Ghi health report, XCom: all_healthy: bool, issues: [str]
- **Mục đích:** Kiểm tra sức khỏe hệ thống (Tiki API, DB, Redis)
- **Operator:** `PythonOperator` với function `health_check_monitoring(**context)` (dòng ~4560)
- **Input:** Các endpoints (env vars)
- **Output:** Health report (XCom); XCom: `{ all_healthy: bool, issues: [str] }`
- **Error handling:** Try-catch; log exceptions; don't fail DAG (monitoring only)
- **Dependencies:** Có thể chạy parallel hoặc sau Task 10.1

#### **Task 11.2: Backup Database**
- **Quy trình:**
  - Thực hiện `pg_dump` trên database `crawl_data`
  - Lưu file backup vào `backups/postgres/{timestamp}.sql.gz`
  - Validate backup integrity (check file size > 1MB)
  - Ghi XCom: backup_path, backup_size
- **Mục đích:** Backup database `crawl_data` sau khi load thành công; disaster recovery
- **Operator:** `PythonOperator` với function `backup_database(**context)` (dòng ~5098)
- **Input:** DB connection params
- **Output:** Backup file `backups/postgres/{timestamp}.sql.gz`; XCom: `{ backup_path: str, backup_size: int }`
- **Cấu hình:** DB connection, backup directory, retention policy
- **Error handling:** Try-catch; log exceptions; don't fail DAG (backup is non-blocking)
- **Dependencies:** Phụ thuộc Task 8.1 (`load_products`) — chạy sau load thành công

#### **Task 11.3: Cleanup Redis Cache**
- **Quy trình:**
  - Connect Redis, lấy tất cả keys
  - Filter keys có TTL <= 7 days hoặc expired
  - Xóa expired keys (DEL command)
  - Ghi XCom: deleted_count
- **Mục đích:** Xóa expired/old caches khỏi Redis; tối ưu memory
- **Operator:** `PythonOperator` với function `cleanup_redis_cache(**context)` (dòng ~4920)
- **Input:** Redis connection params, TTL threshold
- **Output:** XCom: `{ deleted_count: int }`
- **Error handling:** Try-catch; log exceptions; don't fail DAG
- **Dependencies:** Có thể chạy end-of-DAG hoặc parallel

#### **Task 11.4: Cleanup Old Backups**
- **Quy trình:**
  - Liệt kê tất cả files trong `backups/postgres/`
  - Sort theo timestamp (mới nhất first)
  - Giữ tối đa N backups (default 5), xóa cũ hơn
  - Ghi XCom: deleted_files, remaining_count
- **Mục đích:** Giữ tối đa N backups gần nhất; tối ưu disk space
- **Operator:** `PythonOperator` với function `cleanup_old_backups(retention_count=5)` (dòng ~5049)
- **Input:** Backup directory, retention count
- **Output:** XCom: `{ deleted_files: [str], remaining_count: int }`
- **Error handling:** Try-catch; log exceptions; don't fail DAG
- **Dependencies:** Phụ thuộc Task 11.2 (`backup_database`) — chạy sau backup

---

## 📊 Sơ Đồ Dependencies (Task Dependencies)

```
Node 1: load_and_prepare
  ├─ Task 1.1: load_categories
  └─ Task 1.2: (none; Task 1.1 is standalone load)

Node 2: crawl_categories
  └─ Task 2.1: crawl_single_category[category_0..N] ← Dynamic Mapping

Node 3: merge_and_deduplicate
  └─ Task 3.1: merge_products (Fan-in from Node 2)

Node 4: prepare_details
  └─ Task 4.1: prepare_products_for_detail

Node 5: crawl_product_details
  └─ Task 5.1: crawl_product_batch[batch_0..M] ← Dynamic Mapping

Node 6: merge_details
  ├─ Task 6.1: merge_product_details (Fan-in from Node 5)
  └─ Task 6.2: save_products_with_detail (Atomic write)

Node 7: transform_normalize
  └─ Task 7.1: transform_products

Node 8: load_to_db
  └─ Task 8.1: load_products (Upsert)

Node 9: validate_quality
  └─ Task 9.1: validate_data

Node 10: summary_notify
  └─ Task 10.1: aggregate_and_notify (Send Discord)

Node 11: maintenance
  ├─ Task 11.1: health_check_monitoring
  ├─ Task 11.2: backup_database
  ├─ Task 11.3: cleanup_redis_cache
  └─ Task 11.4: cleanup_old_backups

Dependencies:
Task 1.1 → Task 2.1 → Task 3.1 → Task 4.1 → Task 5.1 → Task 6.1 → Task 6.2 → Task 7.1 → Task 8.1 → Task 10.1 → [END]
                                                           ├──→ Task 9.1 ──────↑
Task 11.2 (after 8.1) → Task 11.4
Task 11.1 (parallel after 8.1)
Task 11.3 (end-of-DAG)
```

---

## 🔍 Dynamic Task Mapping Chi Tiết

### Node 2 Expand: `crawl_single_category[category_<id>]`
- **Source data:** Output từ Task 1.1 (`load_categories`) — danh sách categories
- **Map function:** Mỗi category → 1 task `crawl_single_category[category_<id>]`
- **Số tasks:** Động (~50-100 leaf categories)
- **Result:** XCom per task chứa `[{ product_id, name, url, category_url }, ...]`
- **Fan-in:** Task 3.1 (`merge_products`) lấy XCom từ tất cả tasks trong Node 2

### Node 5 Expand: `crawl_product_batch[batch_<idx>]`
- **Source data:** Output từ Task 4.1 (`prepare_products_for_detail`) — danh sách batches
- **Map function:** Mỗi batch → 1 task `crawl_product_batch[batch_<idx>]`
- **Số tasks:** Động (~10-50 tasks tuỳ batch size ~10 products/batch)
- **Result:** XCom per task chứa `[{ product_id, detail_valid: bool }, ...]`
- **Fan-in:** Task 6.1 (`merge_product_details`) lấy XCom từ tất cả tasks trong Node 5

---

## 🎯 Cách Chạy DAG từ Airflow UI

1. **Trigger DAG:** Airflow UI → Tiki → "Play" button → Chạy DAG
2. **Monitor flow:**
   - Tree View: Xem trạng thái 10 runs gần nhất
   - Graph View: Xem sơ đồ dependencies (có thể collapse TaskGroups)
   - Gantt View: Xem timeline task execution
3. **Check XCom:** Admin → XCom → Filter by DAG/Task → Xem dữ liệu truyền
4. **Debug:** Logs → Xem output từng task
5. **Retry:** Task fail → Right-click → "Clear" → Re-run

