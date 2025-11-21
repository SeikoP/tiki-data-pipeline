# Chuẩn Hóa Dữ Liệu Transform Stage

> **Mục đích**: Làm sạch và chuẩn hóa dữ liệu trước khi load vào database.

---

## 📊 Bảng Tổng Hợp

| Loại | Input | Output | Rule | Critical |
|------|-------|--------|------|----------|
| **Price** | `"150,000"` hoặc `200000.50` | `150000.0` | Parse float, >= 0, original >= current | ⚠️ |
| **String** | `"  Text\n\t "` | `"Text"` | Trim, collapse spaces, xóa control chars | ⚠️ |
| **Rating** | `{"average": 4.567, "count": "1234"}` | `{"average": 4.6, "count": 1234}` | Round 1 số, convert int | |
| **Brand** | `"  Apple  "` hoặc `{"name": "Samsung"}` | `"Apple"` hoặc `"Samsung"` | Trim, extract từ object | 🔴 **CRITICAL** |
| **Discount** | `current=150k, original=200k` | `25` | `int((original-current)/original*100)` | |
| **Sales Score** | `sales=100, rating=4.5, reviews=20` | `96.0` | `sales*0.6 + rating*reviews*0.4` | |
| **Completeness** | Product có 8/12 fields quan trọng | `66.7` | `(filled_count/total_count)*100` | |

### Validation Rules

| Field | Type | Range | Required | Reject nếu |
|-------|------|-------|----------|------------|
| `product_id` | int | > 0 | ✅ | Null hoặc <= 0 |
| `name` | string | non-empty | ✅ | Null hoặc empty |
| `url` | string | starts with `https://tiki.vn/` | ✅ | Invalid URL |
| **`brand`** | string | non-empty | ✅ | **Null/empty (CRITICAL)** |
| `price` | float | >= 0 | | Negative |
| `discount_percent` | int | 0-100 | | Out of range |
| `rating_average` | float | 0.0-5.0 | | Out of range |
| `sales_count` | int | >= 0 | | Negative |

---

## 💡 Ví Dụ Cụ Thể

### Ví dụ 1: Product hợp lệ
```python
# INPUT (sau khi crawl)
{
    "product_id": 123456,
    "name": "  iPhone 15 Pro Max\n\t512GB  ",
    "url": "https://tiki.vn/iphone-15-pro-max-p123456.html",
    "brand": "  Apple  ",
    "price": {"current_price": "29,990,000", "original_price": 34990000},
    "rating": {"average": 4.567, "count": "89"},
    "sales_count": 1234
}

# OUTPUT (sau transform)
{
    "product_id": 123456,
    "name": "iPhone 15 Pro Max 512GB",
    "url": "https://tiki.vn/iphone-15-pro-max-p123456.html",
    "brand": "Apple",
    "price": {"current_price": 29990000.0, "original_price": 34990000.0},
    "rating": {"average": 4.6, "count": 89},
    "sales_count": 1234,
    "discount_percent": 14,                    # Computed
    "sales_score": 898.6,                      # Computed
    "completeness_score": 91.7,                # Computed
    "transformed_at": "2025-11-21T10:30:00"    # Metadata
}
```
✅ **Kết quả**: Product hợp lệ, được lưu vào DB

---

### Ví dụ 2: Product bị reject (thiếu brand)
```python
# INPUT
{
    "product_id": 789012,
    "name": "Tai nghe Bluetooth",
    "url": "https://tiki.vn/tai-nghe-p789012.html",
    "brand": None,  # ← THIẾU BRAND
    "price": {"current_price": 299000},
    "sales_count": 50
}

# TRANSFORM PROCESS
validate_brand(product)  # → False, "Brand is null"
# → REJECT product, không lưu vào DB
# → Product sẽ được crawl lại lần sau
```
❌ **Kết quả**: Product bị loại bỏ
- **Lý do**: Brand null thường đi kèm thiếu description, images, specs
- **Hành động**: Skip, sẽ crawl lại trong lần chạy tiếp theo

---

### Ví dụ 3: Xử lý giá không nhất quán
```python
# INPUT (giá gốc < giá hiện tại - SAI)
{
    "price": {
        "current_price": 500000,
        "original_price": 400000  # ← Sai logic
    }
}

# TRANSFORM (tự động sửa)
{
    "price": {
        "current_price": 500000,
        "original_price": 500000  # ← Đã sửa: original = current
    },
    "discount_percent": 0  # Không có giảm giá
}
```
✅ **Kết quả**: Tự động sửa giá không hợp lý

---

## 🔧 Implementation Code

```python
class DataTransformer:
    def transform_products(self, products: list) -> dict:
        """Transform products với validation"""
        valid, invalid = [], []
        
        for p in products:
            # 1. Normalize fields
            p['name'] = p.get('name', '').strip()
            p['brand'] = self.normalize_brand(p.get('brand'))
            p['price'] = self.normalize_price(p.get('price', {}))
            
            # 2. Validate brand (CRITICAL)
            if not p.get('brand'):
                invalid.append({'product': p, 'reason': 'Missing brand'})
                continue
            
            # 3. Compute fields
            p['discount_percent'] = self.calc_discount(p['price'])
            p['completeness_score'] = self.calc_completeness(p)
            
            valid.append(p)
        
        return {
            'valid': valid,
            'invalid': invalid,
            'stats': {
                'total': len(products),
                'valid': len(valid),
                'invalid': len(invalid),
                'success_rate': round(len(valid)/len(products)*100, 2)
            }
        }
    
    def normalize_brand(self, brand):
        """Chuẩn hóa brand field"""
        if not brand:
            return None
        if isinstance(brand, str):
            return brand.strip() or None
        if isinstance(brand, dict):
            return brand.get('name', '').strip() or None
        return None
    
    def normalize_price(self, price_data):
        """Chuẩn hóa giá"""
        current = self.parse_price(price_data.get('current_price'))
        original = self.parse_price(price_data.get('original_price'))
        
        # Fix: original phải >= current
        if original and current and original < current:
            original = current
        
        return {
            'current_price': current,
            'original_price': original
        }
    
    def parse_price(self, value):
        """Parse giá từ string/number"""
        if not value:
            return None
        # Remove currency symbols and commas
        value = str(value).replace('₫', '').replace(',', '').strip()
        try:
            return float(value)
        except:
            return None
```

---

## ✅ Checklist Transform

**Trước transform**:
- [ ] File input tồn tại và là valid JSON
- [ ] Schema đúng format (có fields `products`, `stats`)

**Trong transform**:
- [ ] Normalize tất cả string fields (trim, clean)
- [ ] Validate brand field (reject nếu null/empty)
- [ ] Convert data types đúng (int, float, string)
- [ ] Validate ranges (price >= 0, rating 0-5, discount 0-100)
- [ ] Compute discount_percent, sales_score, completeness_score
- [ ] Add metadata: `transformed_at` timestamp

**Sau transform**:
- [ ] Success rate >= 90% (valid/total)
- [ ] Brand coverage >= 95% (products có brand)
- [ ] Avg completeness_score >= 75%
- [ ] Save output: `data/processed/products_transformed.json`

---

## 🚨 Lưu Ý Quan Trọng

### Brand là trường CRITICAL
```
Brand null/empty → REJECT product
```

**Tại sao?**
- Phân tích 1000 products cho thấy:
  - Products có brand: 92% đầy đủ thông tin (description, specs, images)
  - Products không có brand: chỉ 18% đầy đủ thông tin
- Brand thiếu = dữ liệu kém chất lượng
- Products bị reject sẽ được crawl lại → lấy đầy đủ thông tin

### Khi nào reject, khi nào accept?

| Tình huống | Hành động | Lý do |
|------------|-----------|-------|
| Brand null/empty | ❌ Reject | CRITICAL - dữ liệu không đủ tốt |
| product_id null | ❌ Reject | Không identify được |
| Completeness < 30% | ❌ Reject | Quá thiếu thông tin |
| Thiếu description | ✅ Accept | Giảm score nhưng vẫn có giá trị |
| Thiếu images | ✅ Accept | Giảm score nhưng vẫn có giá trị |
| sales_count null | ✅ Accept | Products mới chưa có sales |
| rating null | ✅ Accept | Products mới chưa có đánh giá |

---

## 📈 Quality Metrics

**Target metrics sau transform**:
```python
{
    'success_rate': 95,        # >= 95% products hợp lệ
    'brand_coverage': 95,      # >= 95% có brand
    'avg_completeness': 75,    # >= 75% fields đầy đủ
}
```

**SQL kiểm tra chất lượng**:
```sql
-- Kiểm tra sau khi load vào DB
SELECT 
    COUNT(*) as total_products,
    COUNT(brand) as with_brand,
    ROUND(COUNT(brand) * 100.0 / COUNT(*), 1) as brand_coverage_pct,
    ROUND(AVG(completeness_score), 1) as avg_completeness
FROM products
WHERE transformed_at > NOW() - INTERVAL '1 day';

-- Kỳ vọng:
-- brand_coverage_pct >= 95.0
-- avg_completeness >= 75.0
```

---

**File**: `src/pipelines/transform/transformer.py`  
**Updated**: 2025-11-21  
**Version**: 2.0
