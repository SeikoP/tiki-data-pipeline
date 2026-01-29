
import requests
import time
import json

def test_tiki_api(product_id):
    url = f"https://tiki.vn/api/v2/products/{product_id}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://tiki.vn/"
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Success for ID: {product_id} ({data.get('name', 'N/A')[:30]}...)")
            
            # Key mappings we're looking for
            mappings = {
                "Product ID": "id",
                "Name": "name",
                "Price": "price",
                "Original Price": "list_price",
                "Discount %": "discount_rate",
                "Rating Avg": "rating_average",
                "Review Count": "review_count",
                "Sales (Quantity Sold)": "all_time_quantity_sold",
                "Sales (Day)": "day_ago_quantity_sold",
                "Quantity Sold": "quantity_sold",
                "Brand": "brand.name",
                "Seller": "current_seller.name"
            }
            
            print("-" * 20)
            for label, key_path in mappings.items():
                keys = key_path.split('.')
                val = data
                for k in keys:
                    if isinstance(val, dict):
                        val = val.get(k)
                    else:
                        val = None
                        break
                print(f"{label}: {val}")
            
            # Check for any field containing 'sold'
            sold_fields = {k: v for k, v in data.items() if 'sold' in k.lower() or 'quantity' in k.lower()}
            if sold_fields:
                print(f"Sold/Quantity fields found: {sold_fields}")
                
            return data
        else:
            print(f"❌ Failed for ID {product_id}! Status: {response.status_code}")
            return None
    except Exception as e:
        print(f"❌ Error for ID {product_id}: {e}")
        return None

if __name__ == "__main__":
    test_ids = ["184059211", "271966786"] 
    for pid in test_ids:
        print(f"\nTesting Product ID: {pid}")
        test_tiki_api(pid)
