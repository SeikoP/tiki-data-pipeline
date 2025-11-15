"""
Demo Step 2: Transform Products

Bước này transform dữ liệu sản phẩm đã crawl:
- Normalize fields (trim, parse numbers)
- Flatten nested structures
- Validate dữ liệu
- Tính computed fields (revenue, popularity score, etc.)
"""

import json
import os
import sys
from pathlib import Path

# Fix encoding cho Windows console
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

# Thêm src vào path
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

# Import transformer
try:
    from pipelines.transform.transformer import DataTransformer
except ImportError as e:
    print(f"❌ Lỗi import: {e}")
    print("💡 Đảm bảo bạn đã cài đặt dependencies: pip install -r requirements.txt")
    sys.exit(1)


def main():
    print("=" * 80)
    print("🔄 DEMO STEP 2: TRANSFORM PRODUCTS")
    print("=" * 80)
    print()
    print("Bước này sẽ:")
    print("  1. Đọc dữ liệu sản phẩm đã crawl")
    print("  2. Transform (normalize, validate, compute fields)")
    print("  3. Lưu kết quả đã transform")
    print()

    # Đọc file từ bước 1
    input_file = project_root / "data" / "raw" / "products" / "demo_products.json"
    
    if not input_file.exists():
        print(f"❌ Không tìm thấy file: {input_file}")
        print("💡 Chạy demo_step1_crawl.py trước!")
        sys.exit(1)

    print(f"📂 Đang đọc file: {input_file}")
    
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        products = data.get("products", [])
        print(f"📊 Tổng số products: {len(products)}")
        print()

        if not products:
            print("❌ Không có products để transform!")
            return

        # Hiển thị product gốc (trước transform)
        print("📝 Product gốc (trước transform):")
        sample_product = products[0]
        print(f"   - product_id: {sample_product.get('product_id')}")
        print(f"   - name: {sample_product.get('name')}")
        print(f"   - sales_count: {sample_product.get('sales_count')} (type: {type(sample_product.get('sales_count'))})")
        if 'price' in sample_product:
            print(f"   - price: {sample_product.get('price')} (nested dict)")
        print()

        # Transform
        print("⏳ Đang transform...")
        transformer = DataTransformer(
            strict_validation=False,
            remove_invalid=True,
            normalize_fields=True
        )

        transformed_products, transform_stats = transformer.transform_products(
            products, validate=True
        )

        print()
        print("=" * 80)
        print("📊 TRANSFORM RESULTS")
        print("=" * 80)
        print(f"✅ Valid products: {transform_stats['valid_products']}")
        print(f"❌ Invalid products: {transform_stats['invalid_products']}")
        print(f"🔄 Duplicates removed: {transform_stats['duplicates_removed']}")
        if transform_stats.get('errors'):
            print(f"⚠️  Errors: {len(transform_stats['errors'])}")
        print("=" * 80)
        print()

        if not transformed_products:
            print("❌ Không có products hợp lệ sau transform!")
            return

        # Hiển thị product sau transform
        print("📝 Product sau transform:")
        transformed_sample = transformed_products[0]
        print(f"   - product_id: {transformed_sample.get('product_id')}")
        print(f"   - name: {transformed_sample.get('name')}")
        print(f"   - sales_count: {transformed_sample.get('sales_count')} (type: {type(transformed_sample.get('sales_count'))})")
        print(f"   - price: {transformed_sample.get('price')} (flatten)")
        print(f"   - rating_average: {transformed_sample.get('rating_average')}")
        print(f"   - review_count: {transformed_sample.get('review_count')}")
        print(f"   - estimated_revenue: {transformed_sample.get('estimated_revenue')}")
        print(f"   - popularity_score: {transformed_sample.get('popularity_score')}")
        print()

        # Lưu vào file
        output_dir = project_root / "data" / "processed"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "demo_products_transformed.json"

        output_data = {
            "transformed_at": str(Path(__file__).stat().st_mtime),
            "source_file": str(input_file),
            "total_products": len(products),
            "transform_stats": transform_stats,
            "products": transformed_products
        }

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)

        print(f"💾 Đã lưu vào: {output_file}")
        print()
        print("✅ Bước 2 hoàn thành! Chạy demo_step3_load.py để tiếp tục.")
        print("=" * 80)

    except Exception as e:
        print(f"❌ Lỗi khi transform: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

