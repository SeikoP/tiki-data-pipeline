# 🌳 Hướng Dẫn Sử Dụng Multiple Root Categories

## Tổng Quan

Script `crawl_categories_optimized.py` hiện hỗ trợ crawl nhiều root categories cùng lúc, giúp bạn crawl toàn bộ cây danh mục từ nhiều điểm bắt đầu.

## Cách Cấu Hình

### Cách 1: Sử dụng File JSON (Khuyến nghị)

Tạo file `data/raw/root_categories.json` với danh sách các root URLs:

```json
[
  "https://tiki.vn/thoi-trang-nam/c915",
  "https://tiki.vn/nha-cua-doi-song/c1883",
  "https://tiki.vn/dien-tu-dien-lanh/c4221"
]
```

Script sẽ tự động đọc file này khi chạy.

**Lưu ý:** Có thể copy từ file mẫu:
```bash
cp data/raw/root_categories.json.example data/raw/root_categories.json
# Sau đó chỉnh sửa file theo nhu cầu
```

### Cách 2: Sử dụng Biến Môi Trường

Set biến môi trường `TIKI_ROOT_CATEGORIES` với các URLs phân cách bởi dấu phẩy:

```bash
export TIKI_ROOT_CATEGORIES="https://tiki.vn/thoi-trang-nam/c915,https://tiki.vn/nha-cua-doi-song/c1883"
python src/pipelines/crawl/crawl_categories_optimized.py
```

Hoặc trong file `.env`:
```env
TIKI_ROOT_CATEGORIES=https://tiki.vn/thoi-trang-nam/c915,https://tiki.vn/nha-cua-doi-song/c1883
```

### Cách 3: Sử dụng Giá Trị Mặc Định

Nếu không có file config và không set biến môi trường, script sẽ sử dụng giá trị mặc định trong code (hiện tại là `https://tiki.vn/thoi-trang-nam/c915`).

## Các Tham Số Khác

Ngoài root categories, bạn có thể cấu hình:

- **TIKI_MAX_CATEGORY_LEVEL**: Độ sâu tối đa (mặc định: 4)
- **TIKI_CRAWL_MAX_WORKERS**: Số thread song song (mặc định: 3)

Ví dụ:
```bash
export TIKI_MAX_CATEGORY_LEVEL=5
export TIKI_CRAWL_MAX_WORKERS=5
python src/pipelines/crawl/crawl_categories_optimized.py
```

## Kết Quả

Tất cả categories từ các root categories sẽ được merge vào một file duy nhất:
- **Output**: `data/raw/categories_recursive_optimized.json`
- **Cache**: `data/raw/cache/` (mỗi URL có một file cache riêng)

## Lưu Ý

1. **Trùng lặp**: Script tự động loại bỏ categories trùng lặp (theo URL)
2. **Cache**: Mỗi root category được cache riêng, giúp tăng tốc khi chạy lại
3. **Thứ tự ưu tiên**: File JSON > Biến môi trường > Giá trị mặc định
4. **Performance**: Crawl song song nhiều root categories có thể tăng tải lên server, nên điều chỉnh `max_workers` phù hợp

## Ví Dụ Sử Dụng

### Crawl 3 root categories với độ sâu 4 level:

```bash
# Tạo file config
cat > data/raw/root_categories.json << EOF
[
  "https://tiki.vn/thoi-trang-nam/c915",
  "https://tiki.vn/nha-cua-doi-song/c1883",
  "https://tiki.vn/dien-tu-dien-lanh/c4221"
]
EOF

# Chạy script
python src/pipelines/crawl/crawl_categories_optimized.py
```

### Crawl với biến môi trường:

```bash
export TIKI_ROOT_CATEGORIES="https://tiki.vn/thoi-trang-nam/c915,https://tiki.vn/nha-cua-doi-song/c1883"
export TIKI_MAX_CATEGORY_LEVEL=5
export TIKI_CRAWL_MAX_WORKERS=4
python src/pipelines/crawl/crawl_categories_optimized.py
```
