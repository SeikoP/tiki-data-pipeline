"""
Script để kiểm tra xem sales_count có trong data không
"""
import json
import os

# Kiểm tra các file data
files_to_check = [
    'data/raw/products/products.json',
    'data/raw/products/products_batch_0.json',
    'data/test_output/test_products_sales_count.json'
]

for file_path in files_to_check:
    if not os.path.exists(file_path):
        print(f"❌ File không tồn tại: {file_path}")
        continue
    
    print(f"\n{'='*70}")
    print(f"File: {file_path}")
    print(f"{'='*70}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Lấy products
        if 'products' in data:
            products = data['products']
        elif isinstance(data, list):
            products = data
        else:
            products = []
        
        if not products:
            print("   ⚠️  Không có products")
            continue
        
        # Kiểm tra sales_count
        total = len(products)
        has_sales_count = 0
        sales_count_types = {}
        sales_count_samples = []
        
        for product in products[:100]:  # Chỉ check 100 đầu tiên
            if 'sales_count' in product:
                has_sales_count += 1
                sales_count_value = product['sales_count']
                sales_count_type = type(sales_count_value).__name__
                sales_count_types[sales_count_type] = sales_count_types.get(sales_count_type, 0) + 1
                
                if len(sales_count_samples) < 5:
                    sales_count_samples.append({
                        'product_id': product.get('product_id'),
                        'sales_count': sales_count_value,
                        'type': sales_count_type
                    })
        
        print(f"   Tong products: {total}")
        print(f"   Co sales_count: {has_sales_count}/{min(100, total)} ({has_sales_count/min(100, total)*100:.1f}%)")
        print(f"   Khong co sales_count: {min(100, total) - has_sales_count}/{min(100, total)}")
        
        if sales_count_types:
            print(f"\n   Loai sales_count:")
            for stype, count in sales_count_types.items():
                print(f"      - {stype}: {count}")
        
        if sales_count_samples:
            print(f"\n   Vi du sales_count:")
            for sample in sales_count_samples:
                print(f"      - Product {sample['product_id']}: {sample['sales_count']} (type: {sample['type']})")
        
        # Hiển thị các fields có trong product
        if products:
            sample_product = products[0]
            print(f"\n   Cac fields trong product:")
            for key in sorted(sample_product.keys()):
                value = sample_product[key]
                value_type = type(value).__name__
                value_preview = str(value)[:50] if value is not None else 'None'
                print(f"      - {key}: {value_type} = {value_preview}")
        
    except Exception as e:
        print(f"   ❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()

print(f"\n{'='*70}")
print("Hoan thanh kiem tra!")
print(f"{'='*70}")

