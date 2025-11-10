# Tối Ưu Crawl Categories - Hướng Dẫn

## 🎯 Vấn Đề

Một số danh mục Tiki có cấu trúc phân cấp rất sâu (nhiều level), một số thì ít. Cần crawl hết tất cả các level để đảm bảo không bỏ sót dữ liệu.

## ✅ Giải Pháp: Crawl Đệ Quy

Đã implement function `crawl_categories_recursive()` để crawl tất cả các level tự động.

### Tính Năng

1. **Crawl đệ quy tự động**: Tự động crawl đến khi không còn sub-categories
2. **Tránh duplicate**: Tracking `visited_ids` để không crawl lại
3. **Tránh circular reference**: Tự động phát hiện và bỏ qua
4. **Progress tracking**: Hiển thị progress theo từng level
5. **Statistics**: Thống kê chi tiết theo level

## 📝 Cách Sử Dụng

### 1. Crawl Đầy Đủ (Tất Cả Các Level)

```python
from pipelines.crawl.tiki.extract_category_link import crawl_all_sub_categories

# Crawl đệ quy tất cả các level, không giới hạn độ sâu
sub_categories = crawl_all_sub_categories(
    categories,
    max_categories=None,      # Crawl tất cả
    recursive=True,            # Bật crawl đệ quy
    max_depth=None            # Không giới hạn độ sâu
)
```

### 2. Crawl Với Giới Hạn Độ Sâu

```python
# Giới hạn độ sâu tối đa 10 levels
sub_categories = crawl_all_sub_categories(
    categories,
    recursive=True,
    max_depth=10              # Tối đa 10 levels
)
```

### 3. Crawl Chỉ 1 Level (Không Đệ Quy)

```python
# Chỉ crawl 1 level, không crawl sâu hơn
sub_categories = crawl_all_sub_categories(
    categories,
    recursive=False           # Tắt crawl đệ quy
)
```

### 4. Crawl Với Giới Hạn Số Lượng

```python
# Crawl tối đa 50 categories mỗi level
sub_categories = crawl_all_sub_categories(
    categories,
    max_categories=50,        # Giới hạn 50 categories/level
    recursive=True,
    max_depth=None
)
```

## ⚙️ Cấu Hình Tối Ưu

### Cho Dữ Liệu Khổng Lồ

```python
# Tối ưu cho crawl toàn bộ Tiki
sub_categories = crawl_all_sub_categories(
    categories,
    max_categories=None,      # Crawl tất cả
    recursive=True,            # Bật đệ quy
    max_depth=None            # Không giới hạn (crawl hết)
)
```

### Cho Test/Demo

```python
# Tối ưu cho test nhanh
sub_categories = crawl_all_sub_categories(
    categories[:5],           # Chỉ 5 categories đầu
    max_categories=10,        # 10 categories/level
    recursive=True,
    max_depth=3              # Chỉ 3 levels
)
```

## 📊 Output & Statistics

Function sẽ hiển thị:
- Progress theo từng level
- Số lượng categories đã crawl
- Số lượng sub-categories tìm thấy
- Phân bố theo level
- Số lỗi (nếu có)

Ví dụ output:
```
[Level 0] Đang crawl 26 root categories...
[1/26] 📂 Thời trang nam (ID: 915, Level: 0)
   ✓ Tìm thấy 20 sub-categories

│[Level 1] Đang crawl 20 categories...
│[1/20] 📂 Áo thun nam (ID: 917, Level: 1)
│   ✓ Tìm thấy 5 sub-categories

││[Level 2] Đang crawl 5 categories...
││[1/5] 📂 Áo thun nam ngắn tay (ID: 5333, Level: 2)
││   - Không tìm thấy sub-categories

[6] Thống kê crawl:
    - Tổng categories đã crawl: 51
    - Tổng sub-categories tìm thấy: 25
    - Unique sub-categories: 25
    - Lỗi: 0
    - Phân bố theo level:
      level_1: 20 categories
      level_2: 5 categories
```

## 🔧 Tối Ưu Hóa

### 1. **Rate Limiting**
Sử dụng rate limiter để tránh bị block:
```python
from utils.rate_limiter import rate_limited

@rate_limited(max_per_minute=30, max_per_hour=1000)
def crawl_sub_categories(...):
    ...
```

### 2. **Parallel Processing**
Có thể chạy song song nhiều categories (trong Airflow DAG):
```python
# Mỗi category crawl song song
crawl_results = crawl_category.expand(category=categories)
```

### 3. **Incremental Crawl**
Chỉ crawl những gì thay đổi:
- So sánh với metadata trước đó
- Chỉ crawl categories mới hoặc thay đổi

### 4. **Caching**
Cache kết quả crawl để tránh crawl lại:
- Lưu vào file JSON
- Check timestamp để biết có cần crawl lại không

## ⚠️ Lưu Ý

1. **Circular Reference**: Function tự động phát hiện và bỏ qua
2. **Max Depth**: Nên set giới hạn hợp lý (10-15) để tránh crawl quá sâu
3. **Memory**: Với dữ liệu lớn, có thể cần xử lý theo batch
4. **Time**: Crawl đệ quy có thể mất nhiều thời gian, nên chạy trong background

## 🚀 Best Practices

1. **Bắt đầu với max_depth nhỏ** để test
2. **Tăng dần max_depth** khi đã verify
3. **Monitor progress** để biết đang ở đâu
4. **Lưu checkpoint** để có thể resume nếu bị gián đoạn
5. **Sử dụng Airflow** để schedule và monitor

## 📈 Performance

- **1 level**: ~1-2 phút cho 26 categories
- **3 levels**: ~5-10 phút
- **Tất cả levels**: ~30-60 phút (tùy số lượng)

## 🔍 Debug

Nếu gặp vấn đề:
1. Check logs để xem đang crawl đến level nào
2. Kiểm tra circular references
3. Verify Firecrawl API đang hoạt động
4. Check rate limiting có bị block không

