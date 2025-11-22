"""
Script load tất cả products từ file merged vào database
Sử dụng Transform → Load pipeline
"""

import json
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from pipelines.transform.transformer import DataTransformer
from pipelines.load.loader import DataLoader

def main():
    print("=" * 80)
    print("💾 LOAD ALL PRODUCTS TO DATABASE")
    print("=" * 80)
    print()
    
    # File input
    input_file = project_root / "data" / "raw" / "all_products_merged.json"
    
    if not input_file.exists():
        print(f"❌ File không tồn tại: {input_file}")
        print("💡 Chạy: .\\scripts\\merge-all-products.ps1")
        return
    
    # 1. Đọc dữ liệu
    print(f"📖 Đọc file: {input_file}")
    with open(input_file, 'r', encoding='utf-8') as f:
        raw_products = json.load(f)
    
    print(f"✅ Đọc xong: {len(raw_products)} products")
    
    # 2. Transform
    print(f"\n🔄 Transform dữ liệu...")
    transformer = DataTransformer()
    transformed, transform_stats = transformer.transform_products(raw_products)
    print(f"✅ Transform xong: {len(transformed)} products hợp lệ")
    
    if transform_stats['invalid_products'] > 0:
        print(f"⚠️  {transform_stats['invalid_products']} products không hợp lệ đã bị loại bỏ")
        if transform_stats['errors']:
            print(f"   Lỗi đầu tiên: {transform_stats['errors'][0]}")
    
    # 3. Load vào database
    print(f"\n💾 Load vào PostgreSQL database...")
    loader = DataLoader()
    
    try:
        result = loader.load_products(transformed)
        
        print("\n" + "=" * 80)
        print("✅ HOÀN TẤT!")
        print("=" * 80)
        print(f"📊 Inserted: {result.get('inserted', 0)}")
        print(f"🔄 Updated: {result.get('updated', 0)}")
        print(f"⏱️  Duration: {result.get('duration', 0):.2f}s")
        
        if result.get('errors'):
            print(f"\n⚠️  Có {len(result['errors'])} lỗi:")
            for err in result['errors'][:10]:
                print(f"   - {err}")
        
        print(f"\n💡 Kiểm tra database:")
        print(f"   docker exec tiki-data-pipeline-postgres-1 psql -U postgres -d crawl_data -c \"SELECT COUNT(*) FROM products;\"")
        
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
