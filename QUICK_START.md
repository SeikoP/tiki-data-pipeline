# Tiki Data Pipeline - Quick Start Guide

## 🚀 Chạy Demo (Nhanh Nhất)

### 1. Chỉ xem dữ liệu đã có (~1-2 giây)
```bash
python scripts/test_crawl_demo.py
# Đặt SKIP_CRAWL=True trong code
```

### 2. Validate cấu trúc hierarchical
```bash
python scripts/validate_hierarchical.py
```
**Output**: ✅ Valid: True, 0 errors, 67 categories

### 3. Chạy đầy đủ (với cache)
```bash
python scripts/test_crawl_demo.py
# Lần đầu: ~30-60s (crawl từ Firecrawl API)
# Lần sau: ~5-10s (load từ cache)
```

## 📊 Cấu Trúc Dữ Liệu

### Demo Files
```
data/raw/demo/
├── demo_categories.json              # 3 root categories (Sách, English, Fashion)
├── demo_sub_categories.json          # 72 sub-categories
├── demo_hierarchical.json            # ✅ VALIDATED - Cấu trúc phân cấp đúng
├── demo_categories_cache.json        # Cache
└── demo_sub_categories_cache_*.json  # Cache per category
```

### Hierarchical Structure (correct format)
```json
[
  {
    "name": "Sách tiếng Việt",
    "category_id": "316",
    "parent_id": null,                    // ROOT has null parent
    "parent_name": null,
    "sub_categories": [
      {
        "name": "Sách thiếu nhi",
        "category_id": "393",
        "parent_id": "316",               // MATCHES parent's category_id
        "parent_name": "Sách tiếng Việt", // MATCHES parent's name
        "sub_categories": [
          {
            "name": "Đạo đức - Kỹ năng sống",
            "category_id": "852",
            "parent_id": "393",           // MATCHES parent's category_id
            "parent_name": "Sách thiếu nhi",
            "sub_categories": []
          }
        ]
      }
    ]
  }
]
```

## 🔍 Validation Checks

Tất cả checks này đều **PASSED** ✅:

- [x] No duplicates (cùng category_id không xuất hiện 2 lần)
- [x] All categories included (không mất dữ liệu)
- [x] parent_id matches (parent_id = parent's category_id)
- [x] No circular references (A không thể là con của chính nó)
- [x] Correct structure (sub_categories lồng đúng)

## 🛠️ Tối Ưu Hóa Settings

Edit `scripts/test_crawl_demo.py` line ~370:

```python
# CẤU HÌNH - Điều chỉnh để chạy nhanh/chậm
USE_CACHE = True              # True = nhanh hơn
SKIP_CRAWL = False            # True = không crawl, chỉ load cache
MAX_CATEGORIES = 1            # Giảm để nhanh hơn
MAX_DEPTH = 2                 # Giảm độ sâu để nhanh
MAX_CATEGORIES_PER_LEVEL = 5  # Giảm để nhanh
SKIP_BUILD_HIERARCHICAL = False  # True = bỏ qua bước này
```

### Presets

**Ultra Fast (1-2s)**
```python
USE_CACHE = True
SKIP_CRAWL = True
```

**Fast (5-10s)**
```python
USE_CACHE = True
MAX_DEPTH = 1
SKIP_BUILD_HIERARCHICAL = False
```

**Standard (30-60s)**
```python
USE_CACHE = False
MAX_DEPTH = 2
MAX_CATEGORIES = 1
```

**Full (5-10 min)**
```python
USE_CACHE = False
MAX_DEPTH = None  # Không giới hạn
MAX_CATEGORIES = None
```

## 📈 Statistics

### Demo Data
- Root categories: 3
- Total sub-categories: 72
- Total all levels: 67 unique
- Max depth: 3

### Validation Result
```
✅ Valid: True
  - Total collected: 67/67 (100%)
  - Missing: 0
  - Errors: 0
  - Max depth: 3
```

## 🔧 Functions Reference

### Xây dựng cấu trúc phân cấp
```python
from src.pipelines.crawl.tiki.extract_category_link import build_hierarchical_structure

hierarchical = build_hierarchical_structure(all_categories)
```

### Validate cấu trúc
```python
from src.pipelines.crawl.tiki.extract_category_link import validate_hierarchical_structure

result = validate_hierarchical_structure(hierarchical, all_categories)
print(f"Valid: {result['is_valid']}")
print(f"Stats: {result['stats']}")
if result['errors']:
    print(f"Errors: {result['errors']}")
```

### Load/Save JSON
```python
from src.pipelines.crawl.tiki.extract_category_link import load_categories_from_json
import json

# Load
categories = load_categories_from_json("data/raw/demo/demo_hierarchical.json")

# Save
with open("data/raw/output.json", "w", encoding="utf-8") as f:
    json.dump(categories, f, indent=2, ensure_ascii=False)
```

## ❓ FAQ

**Q: File demo_hierarchical.json giờ có đúng không?**
A: ✅ Có! Validation Result: True, 0 errors

**Q: Tại sao có duplicate categories?**
A: Categories xuất hiện ở multiple levels được loại bỏ, chỉ keep latest version

**Q: Script chạy lâu nhất?**
A: Crawl đầu tiên từ Firecrawl API (~30-60s), sau đó load cache (~5-10s)

**Q: Có thể skip validation không?**
A: Có, nhưng không recommend. Validation giúp phát hiện bugs sớm

**Q: Cấu trúc hierarchical có limit độ sâu không?**
A: Không hard limit, nhưng có MAX_DEPTH config để optimize

## 📞 Support

Lỗi? Debug:
```bash
# 1. Kiểm tra file tồn tại
ls data/raw/demo/

# 2. Validate file
python scripts/validate_hierarchical.py

# 3. Xem chi tiết lỗi
python scripts/test_crawl_demo.py
# Check error messages in output
```

---

**Last Updated**: 2025-11-10
**Status**: ✅ All systems operational

