"""
Test extract product details bằng cách tự động gọi các API endpoints của Tiki
"""
import os
import sys
import json
from datetime import datetime

# Load .env file
try:
    from dotenv import load_dotenv
    env_path = os.path.join(os.path.dirname(__file__), '..', '.env')
    if os.path.exists(env_path):
        load_dotenv(env_path)
    else:
        load_dotenv()
except ImportError:
    pass

# Thêm path để import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from pipelines.crawl.tiki.extract_product_details import extract_product_details_from_api_endpoints
from pipelines.crawl.tiki.extract_products import extract_product_id

# Fix encoding on Windows
if sys.platform == "win32":
    try:
        if not hasattr(sys.stdout, 'buffer') or (hasattr(sys.stdout, 'encoding') and sys.stdout.encoding != 'utf-8'):
            import io
            if not isinstance(sys.stdout, io.TextIOWrapper):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass


def safe_print(*args, **kwargs):
    """Safe print function"""
    try:
        print(*args, **kwargs)
    except (ValueError, OSError):
        try:
            print(*args, **kwargs, file=sys.stderr)
        except:
            pass


def main():
    # URL test
    test_url = "https://tiki.vn/apple-iphone-13-hang-chinh-hang-p184059211.html"
    product_id = extract_product_id(test_url)
    
    safe_print("=" * 70)
    safe_print("TEST EXTRACT FROM API ENDPOINTS")
    safe_print("=" * 70)
    safe_print(f"URL: {test_url}")
    safe_print(f"Product ID: {product_id}")
    safe_print()
    
    # Extract từ API endpoints
    safe_print("=" * 70)
    safe_print("BƯỚC 1: Gọi các API endpoints của Tiki")
    safe_print("=" * 70)
    
    result = extract_product_details_from_api_endpoints(test_url, product_id, timeout=30)
    
    if result:
        safe_print("✓ Extract thành công từ API endpoints!")
        safe_print()
        
        # Output JSON
        output_json = json.dumps(result, indent=2, ensure_ascii=False)
        safe_print(output_json)
        
        # Lưu kết quả
        output_dir = os.path.join(os.path.dirname(__file__), '../data/raw/demo')
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, 'test_iphone13_api_endpoints.json')
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        safe_print()
        safe_print(f"💾 Đã lưu kết quả vào: {output_file}")
    else:
        safe_print("✗ Extract thất bại")
        safe_print()
        safe_print("Có thể do:")
        safe_print("  - API endpoints không khả dụng")
        safe_print("  - Product ID không tồn tại")
        safe_print("  - Network error")


if __name__ == "__main__":
    main()

