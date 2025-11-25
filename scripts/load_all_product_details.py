"""
Script để load tất cả file product detail từ cache vào database
Sử dụng Transform → Load pipeline
"""

import json
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.pipelines.transform.transformer import DataTransformer
from src.pipelines.load.loader import DataLoader

def main():
    # Đường dẫn đến thư mục chứa cache
    cache_dir = Path("data/raw/products/detail/cache")
    
    if not cache_dir.exists():
        print(f"❌ Thư mục không tồn tại: {cache_dir}")
        return
    
    # Lấy tất cả file JSON
    json_files = list(cache_dir.glob("*.json"))
    print(f"🔍 Tìm thấy {len(json_files)} file product detail")
    
    if len(json_files) == 0:
        print("❌ Không có file nào để load")
        return
    
    # Đọc tất cả products
    all_products = []
    errors = []
    
    print(f"\n📖 Đang đọc {len(json_files)} files...")
    for i, json_file in enumerate(json_files, 1):
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
                # Kiểm tra cấu trúc dữ liệu
                if isinstance(data, dict):
                    # Single product
                    all_products.append(data)
                elif isinstance(data, list):
                    # Multiple products
                    all_products.extend(data)
                    
            if i % 100 == 0:
                print(f"   Đã đọc {i}/{len(json_files)} files...")
                
        except Exception as e:
            errors.append(f"{json_file.name}: {str(e)}")
    
    print(f"✅ Đọc xong! Tổng: {len(all_products)} products")
    
    if errors:
        print(f"\n⚠️  Có {len(errors)} file lỗi:")
        for err in errors[:10]:  # Show first 10 errors
            print(f"   {err}")
    
    if len(all_products) == 0:
        print("❌ Không có product nào để transform")
        return
    
    # Transform dữ liệu
    print(f"\n🔄 Đang transform {len(all_products)} products...")
    transformer = DataTransformer()
    transformed_products = transformer.transform_products(all_products)
    print(f"✅ Transform xong! {len(transformed_products)} products hợp lệ")
    
    # Load vào database
    print(f"\n💾 Đang load vào database...")
    loader = DataLoader()
    
    try:
        result = loader.load_products(transformed_products)
        
        print(f"\n✅ HOÀN TẤT!")
        print(f"   📊 Inserted: {result.get('inserted', 0)}")
        print(f"   🔄 Updated: {result.get('updated', 0)}")
        print(f"   ⏱️  Duration: {result.get('duration', 0):.2f}s")
        
        if result.get('errors'):
            print(f"   ⚠️  Errors: {len(result['errors'])}")
            for err in result['errors'][:5]:
                print(f"      {err}")
        
    except Exception as e:
        print(f"❌ Lỗi khi load: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
