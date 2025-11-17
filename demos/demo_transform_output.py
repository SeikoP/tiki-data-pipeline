"""
Demo script để minh họa dữ liệu trước và sau khi transform
"""

import json
import os
import sys

# Fix encoding cho Windows console
if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Thêm src vào path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
src_path = os.path.join(project_root, "src")
pipelines_path = os.path.join(src_path, "pipelines")
transform_path = os.path.join(pipelines_path, "transform")

for path in [project_root, src_path, pipelines_path, transform_path]:
    if path not in sys.path:
        sys.path.insert(0, path)

# Import transformer
import importlib.util  # noqa: E402

transformer_path = os.path.join(transform_path, "transformer.py")
spec = importlib.util.spec_from_file_location("transformer", transformer_path)
transformer_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(transformer_module)
DataTransformer = transformer_module.DataTransformer


def create_sample_raw_product():
    """Tạo product raw data (dữ liệu từ crawl)"""
    return {
        "product_id": "123456789",
        "name": "  Máy lọc không khí Xiaomi Mi Air Purifier 4  ",
        "brand": "Thương hiệu: Xiaomi",
        "url": "https://tiki.vn/may-loc-khong-khi-xiaomi-mi-air-purifier-4-p123456789.html?spid=123456",
        "image_url": "https://salt.tikicdn.com/cache/280x280/ts/product/12/34/56/example.jpg",
        "category_url": "https://tiki.vn/dien-tu-dien-lanh/c123",
        "sales_count": "1.234",  # String format
        "price": {
            "currency": "VND",
            "current_price": "2,990,000",  # String với dấu phẩy
            "original_price": "3,500,000",
            "discount_percent": 14.57,  # Sẽ được tính lại
        },
        "rating": {
            "average": "4.5",  # String
            "total_reviews": "1,234",  # String với dấu phẩy
            "rating_distribution": {"5": 800, "4": 300, "3": 100, "2": 24, "1": 10},
        },
        "stock": {"quantity": 50, "available": True, "stock_status": "in_stock"},
        "seller": {"name": "Tiki Trading", "seller_id": "seller_123", "is_official": True},
        "shipping": {"delivery_time": "2-3 ngày", "fast_delivery": True, "free_shipping": True},
        "description": "Máy lọc không khí Xiaomi với công nghệ tiên tiến...",
        "specifications": {
            "Kích thước": "260 x 260 x 735 mm",
            "Công suất": "38W",
            "Diện tích phòng": "48 m²",
            "Bộ lọc": "HEPA + Than hoạt tính",
        },
        "images": [
            "https://example.com/img1.jpg",
            "https://example.com/img2.jpg",
            "https://example.com/img3.jpg",
        ],
        "crawled_at": "2025-01-15 14:30:00",  # String format
        "detail_crawled_at": "2025-01-15T14:35:00.000000",  # ISO format
        "_metadata": {"extraction_method": "selenium", "crawl_version": "1.0"},
    }


def main():
    print("=" * 80)
    print("📊 DEMO: DỮ LIỆU TRƯỚC VÀ SAU TRANSFORM")
    print("=" * 80)

    # Dữ liệu gốc từ crawl
    raw_product = create_sample_raw_product()

    print("\n" + "=" * 80)
    print("📥 DỮ LIỆU GỐC (RAW DATA - Từ Crawl)")
    print("=" * 80)
    print(json.dumps(raw_product, ensure_ascii=False, indent=2))

    # Transform
    transformer = DataTransformer(
        strict_validation=False, remove_invalid=True, normalize_fields=True
    )

    transformed_product = transformer.transform_product(raw_product)

    print("\n" + "=" * 80)
    print("📤 DỮ LIỆU SAU TRANSFORM (TRANSFORMED DATA - Sẵn sàng cho Database)")
    print("=" * 80)
    print(json.dumps(transformed_product, ensure_ascii=False, indent=2))

    print("\n" + "=" * 80)
    print("🔍 CÁC THAY ĐỔI CHÍNH")
    print("=" * 80)

    changes = []

    # So sánh các trường
    if raw_product.get("name") != transformed_product.get("name"):
        changes.append(
            {
                "field": "name",
                "before": f"'{raw_product.get('name')}'",
                "after": f"'{transformed_product.get('name')}'",
                "note": "Đã trim whitespace",
            }
        )

    if raw_product.get("brand") != transformed_product.get("brand"):
        changes.append(
            {
                "field": "brand",
                "before": f"'{raw_product.get('brand')}'",
                "after": f"'{transformed_product.get('brand')}'",
                "note": "Đã loại bỏ prefix 'Thương hiệu: '",
            }
        )

    if raw_product.get("sales_count") != transformed_product.get("sales_count"):
        changes.append(
            {
                "field": "sales_count",
                "before": f"{raw_product.get('sales_count')} (string)",
                "after": f"{transformed_product.get('sales_count')} (int)",
                "note": "Đã parse từ string sang int",
            }
        )

    # Price fields
    if raw_product.get("price", {}).get("current_price") != transformed_product.get("price"):
        changes.append(
            {
                "field": "price",
                "before": f"{raw_product.get('price', {}).get('current_price')} (nested dict)",
                "after": f"{transformed_product.get('price')} (flatten)",
                "note": "Đã flatten từ nested dict, parse từ string sang float",
            }
        )

    if raw_product.get("price", {}).get("discount_percent") != transformed_product.get(
        "discount_percent"
    ):
        changes.append(
            {
                "field": "discount_percent",
                "before": f"{raw_product.get('price', {}).get('discount_percent')} (trong dict)",
                "after": f"{transformed_product.get('discount_percent')} (flatten, tính lại)",
                "note": "Đã tính lại từ price và original_price, làm tròn",
            }
        )

    # Rating fields
    if raw_product.get("rating", {}).get("average") != transformed_product.get("rating_average"):
        changes.append(
            {
                "field": "rating_average",
                "before": f"{raw_product.get('rating', {}).get('average')} (nested dict, string)",
                "after": f"{transformed_product.get('rating_average')} (flatten, float)",
                "note": "Đã flatten từ nested dict, parse từ string sang float",
            }
        )

    if raw_product.get("rating", {}).get("total_reviews") != transformed_product.get(
        "review_count"
    ):
        changes.append(
            {
                "field": "review_count",
                "before": f"{raw_product.get('rating', {}).get('total_reviews')} (nested dict, string)",
                "after": f"{transformed_product.get('review_count')} (flatten, int)",
                "note": "Đã flatten, đổi tên từ total_reviews -> review_count, parse sang int",
            }
        )

    # Seller fields
    if raw_product.get("seller", {}).get("name") != transformed_product.get("seller_name"):
        changes.append(
            {
                "field": "seller_name",
                "before": f"'{raw_product.get('seller', {}).get('name')}' (nested dict)",
                "after": f"'{transformed_product.get('seller_name')}' (flatten)",
                "note": "Đã flatten từ seller.name sang seller_name",
            }
        )

    if raw_product.get("seller", {}).get("seller_id") != transformed_product.get("seller_id"):
        changes.append(
            {
                "field": "seller_id",
                "before": f"'{raw_product.get('seller', {}).get('seller_id')}' (nested dict)",
                "after": f"'{transformed_product.get('seller_id')}' (flatten)",
                "note": "Đã flatten từ seller.seller_id",
            }
        )

    # crawled_at
    if raw_product.get("crawled_at") or raw_product.get("detail_crawled_at"):
        changes.append(
            {
                "field": "crawled_at",
                "before": f"'{raw_product.get('crawled_at')}' hoặc '{raw_product.get('detail_crawled_at')}' (string)",
                "after": f"'{transformed_product.get('crawled_at')}' (ISO format string)",
                "note": "Đã parse và convert sang ISO format string",
            }
        )

    # In các thay đổi
    for i, change in enumerate(changes, 1):
        print(f"\n{i}. {change['field'].upper()}")
        print(f"   Trước: {change['before']}")
        print(f"   Sau:   {change['after']}")
        print(f"   Lý do: {change['note']}")

    # Hiển thị computed fields mới
    print("\n" + "=" * 80)
    print("💰 CÁC TRƯỜNG TÍNH TOÁN MỚI (COMPUTED FIELDS)")
    print("=" * 80)

    if transformed_product:
        computed_fields = [
            ("estimated_revenue", "Doanh thu ước tính", "VND", "sales_count * price"),
            ("price_savings", "Số tiền tiết kiệm", "VND", "original_price - price"),
            ("discount_amount", "Số tiền giảm", "VND", "original_price - price"),
            ("price_category", "Phân loại giá", "", "budget/mid-range/premium/luxury"),
            (
                "popularity_score",
                "Điểm độ phổ biến",
                "0-100",
                "sales_count(50%) + rating(30%) + reviews(20%)",
            ),
            ("value_score", "Điểm giá trị", "", "rating / (price / 1M)"),
            ("sales_velocity", "Tốc độ bán", "", "sales_count"),
        ]

        for field_name, description, unit, formula in computed_fields:
            value = transformed_product.get(field_name)
            if value is not None:
                if unit:
                    print(f"\n{field_name.upper()}:")
                    print(
                        f"   Giá trị: {value:,.2f} {unit}"
                        if isinstance(value, (int, float))
                        else f"   Giá trị: {value} {unit}"
                    )
                    print(f"   Mô tả: {description}")
                    print(f"   Công thức: {formula}")
                else:
                    print(f"\n{field_name.upper()}:")
                    print(f"   Giá trị: {value}")
                    print(f"   Mô tả: {description}")
                    print(f"   Công thức: {formula}")

    print("\n" + "=" * 80)
    print("📋 CẤU TRÚC DATABASE SCHEMA")
    print("=" * 80)
    print(
        """
Các trường chính trong database (table products):

📦 Basic Fields:
- product_id (VARCHAR) - ID sản phẩm
- name (VARCHAR) - Tên sản phẩm (đã normalize)
- url (TEXT) - URL sản phẩm
- image_url (TEXT) - URL hình ảnh
- category_url (TEXT) - URL category
- description (TEXT) - Mô tả
- specifications (JSONB) - Thông số kỹ thuật
- images (JSONB) - Danh sách hình ảnh
- crawled_at (TIMESTAMP) - Thời gian crawl
- updated_at (TIMESTAMP) - Thời gian update (auto)

💰 Price Fields (đã flatten từ nested dict):
- sales_count (INTEGER) - Số lượng đã bán
- price (DECIMAL) - Giá hiện tại
- original_price (DECIMAL) - Giá gốc
- discount_percent (INTEGER) - % giảm giá

⭐ Rating Fields (đã flatten từ nested dict):
- rating_average (DECIMAL) - Rating trung bình (0-5)
- review_count (INTEGER) - Số review

👤 Seller Fields (đã flatten từ nested dict):
- seller_name (VARCHAR) - Tên người bán/shop
- seller_id (VARCHAR) - ID người bán/shop
- seller_is_official (BOOLEAN) - Có phải seller chính thức

🏷️  Brand & Stock Fields:
- brand (VARCHAR) - Thương hiệu sản phẩm
- stock_available (BOOLEAN) - Còn hàng không
- stock_quantity (INTEGER) - Số lượng tồn kho
- stock_status (VARCHAR) - Trạng thái tồn kho
- shipping (JSONB) - Thông tin vận chuyển

📊 Computed Fields (MỚI - được tính toán):
- estimated_revenue (DECIMAL) - Doanh thu ước tính = sales_count * price
- price_savings (DECIMAL) - Số tiền tiết kiệm = original_price - price
- discount_amount (DECIMAL) - Số tiền giảm = original_price - price
- price_category (VARCHAR) - Phân loại: budget/mid-range/premium/luxury
- popularity_score (DECIMAL) - Điểm độ phổ biến (0-100)
- value_score (DECIMAL) - Điểm giá trị = rating / (price / 1M)
- sales_velocity (INTEGER) - Tốc độ bán = sales_count
    """
    )

    print("\n" + "=" * 80)
    print("✅ TÓM TẮT")
    print("=" * 80)
    print(
        """
1. ✅ Normalize: Trim whitespace, loại bỏ prefix không cần thiết
2. ✅ Parse: Convert string numbers sang int/float
3. ✅ Flatten: Chuyển nested dict (price, rating) sang flat structure
4. ✅ Compute: Tính lại các giá trị (discount_percent)
5. ✅ Format: Convert datetime sang ISO format string
6. ✅ Validate: Kiểm tra required fields và format
7. ✅ Type conversion: Đảm bảo types đúng với database schema
    """
    )

    print("=" * 80)


if __name__ == "__main__":
    main()
