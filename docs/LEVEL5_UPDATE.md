# Cập nhật hỗ trợ 5 cấp độ danh mục (Level 5)

## Vấn đề
Dữ liệu hiện tại có tối đa 4 cấp độ từ breadcrumb Tiki. Khi thêm danh mục cha vào, nó trở thành 5 cấp độ, nhưng dữ liệu bị mất vì schema chỉ hỗ trợ 4 cấp độ.

## Giải pháp
Tăng `MAX_CATEGORY_LEVELS` từ 4 lên 5 và thêm `level_5` column vào schema warehouse.

## Các thay đổi

### 1. Cập nhật `src/pipelines/crawl/crawl_products_detail.py`
- **Dòng 609**: Thay đổi `MAX_CATEGORY_LEVELS` từ 4 → 5 (khi extract từ breadcrumb)
- **Dòng 953**: Thay đổi `MAX_CATEGORY_LEVELS` từ 4 → 5 (khi enforce safety check)
- **Dòng 1109**: Thay đổi `MAX_CATEGORY_LEVELS` từ 4 → 5 (trong async fallback)
- **Dòng 977-987**: Sửa logic prepend parent_category để kiểm tra không vượt quá 5 levels

### 2. Cập nhật `src/pipelines/warehouse/star_schema_builder.py`
- **Dòng 183**: Thêm `level_5 VARCHAR(255)` column vào schema `dim_category`
- **Dòng 371-372**: Thêm logic extract `level_5` từ `category_path[4]`
- **Dòng 386**: Cập nhật SQL INSERT để bao gồm `level_5`
- **Dòng 390**: Cập nhật SQL ON CONFLICT để bao gồm `level_5`

### 3. Cập nhật verification scripts
- **`verifydata/verify_warehouse_with_path.py`**: 
  - Thêm `level_5` vào SELECT query
  - Cập nhật logic verify để so sánh 5 levels
  
- **`verifydata/test_auto_parent_detection.py`**:
  - Cập nhật kiểm tra để chấp nhận 4-5 levels (thay vì chỉ 4)

### 4. Tạo migration script
- **`airflow/setup/add_level5_column.sql`**: Migration script để thêm `level_5` column nếu database đã tồn tại

## Ví dụ dữ liệu

### Trước (4 levels):
```json
["Trang trí nhà cửa", "Tranh trang trí", "Tranh đá"]
// Hoặc 4 levels từ breadcrumb
["Trang trí nhà cửa", "Tranh trang trí", "Tranh đá", "Tranh khác"]
```

### Sau (5 levels):
```json
["Trang trí nhà cửa", "Tranh trang trí", "Tranh đá", "Tranh khác", "..."] // Max 5 levels
// Khi có parent:
["Nhà cửa - Đời sống", "Trang trí nhà cửa", "Tranh trang trí", "Tranh đá", "Tranh khác"]
```

## Kiểm tra sau cập nhật

1. **Với dữ liệu hiện tại**: Nếu database đã tồn tại, chạy:
   ```bash
   psql -U postgres -d tiki_warehouse -f airflow/setup/add_level5_column.sql
   ```

2. **Verify dữ liệu**: Chạy script kiểm tra
   ```bash
   python verifydata/verify_warehouse_with_path.py
   ```

3. **Test auto-parent detection**:
   ```bash
   python verifydata/test_auto_parent_detection.py
   ```

## Lưu ý quan trọng
- `MAX_CATEGORY_LEVELS = 5` là giới hạn an toàn để tránh lưu tên sản phẩm vào `category_path`
- Nếu breadcrumb có >5 phần tử, cung cấp danh mục cha, các phần tử cuối cùng sẽ bị cắt
- Schema vẫn hỗ trợ backward compatibility - các category path 3-4 levels vẫn hoạt động bình thường
