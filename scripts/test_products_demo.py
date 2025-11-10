"""
Demo script để test crawl products từ demo categories
Chạy nhanh với dữ liệu nhỏ để xem quy trình
"""
import os
import sys
import json
from datetime import datetime

# Thêm path để import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from pipelines.crawl.tiki.extract_products import (
    crawl_products_from_category,
    crawl_products_from_categories,
    save_products_to_json,
    load_products_from_json
)
from pipelines.crawl.tiki.extract_category_link import load_categories_from_json

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
    """Safe print function - fallback to stderr if stdout fails"""
    try:
        print(*args, **kwargs)
    except (ValueError, OSError):
        try:
            print(*args, **kwargs, file=sys.stderr)
        except:
            pass


def print_section(title):
    """Print section header"""
    safe_print("\n" + "=" * 70)
    safe_print(f"  {title}")
    safe_print("=" * 70)


def print_product_info(product, indent=0):
    """Print product information"""
    indent_str = "  " * indent
    name = product.get('name', 'N/A')
    product_id = product.get('product_id', 'N/A')
    url = product.get('url', 'N/A')
    category_name = product.get('category_name', 'N/A')
    
    safe_print(f"{indent_str}📦 {name}")
    safe_print(f"{indent_str}   ID: {product_id}")
    safe_print(f"{indent_str}   URL: {url}")
    if category_name != 'N/A':
        safe_print(f"{indent_str}   Category: {category_name}")


def demo_load_categories():
    """Load demo categories"""
    print_section("BƯỚC 1: Load Demo Categories")
    
    # Load từ demo files
    demo_categories_file = "data/raw/demo/demo_categories.json"
    demo_sub_categories_file = "data/raw/demo/demo_sub_categories.json"
    
    categories = []
    
    # Load root categories
    if os.path.exists(demo_categories_file):
        root_cats = load_categories_from_json(demo_categories_file)
        categories.extend(root_cats)
        safe_print(f"✓ Đã load {len(root_cats)} root categories")
    
    # Load sub-categories (chỉ lấy một vài để demo nhanh)
    if os.path.exists(demo_sub_categories_file):
        sub_cats = load_categories_from_json(demo_sub_categories_file)
        # Chỉ lấy 2-3 sub-categories đầu để demo nhanh
        demo_sub_cats = sub_cats[:3]
        categories.extend(demo_sub_cats)
        safe_print(f"✓ Đã load {len(demo_sub_cats)} sub-categories (từ {len(sub_cats)} total)")
    
    if not categories:
        safe_print("⚠️  Không tìm thấy demo categories")
        return []
    
    safe_print(f"\n📊 Tổng cộng: {len(categories)} categories để crawl products")
    
    # Hiển thị sample
    safe_print("\n📋 Sample categories:")
    for i, cat in enumerate(categories[:3], 1):
        safe_print(f"  {i}. {cat.get('name', 'N/A')} (ID: {cat.get('category_id', 'N/A')})")
        safe_print(f"     URL: {cat.get('url', 'N/A')}")
    
    if len(categories) > 3:
        safe_print(f"  ... và {len(categories) - 3} categories khác")
    
    return categories


def demo_crawl_products(categories, use_cache=True, max_categories=2, max_products_per_category=5):
    """Demo crawl products từ categories"""
    print_section("BƯỚC 2: Crawl Products từ Categories")
    
    # Cache file
    cache_file = "data/raw/demo/demo_products_cache.json"
    
    # Check cache
    if use_cache and os.path.exists(cache_file):
        safe_print("💡 Tìm thấy cache, đang load từ cache...")
        try:
            cached_products = load_products_from_json(cache_file)
            safe_print(f"✓ Đã load {len(cached_products)} products từ cache")
            return cached_products
        except Exception as e:
            safe_print(f"⚠️  Lỗi khi load cache: {e}, sẽ crawl mới...")
    
    safe_print(f"💡 Đang crawl products từ {max_categories} categories đầu tiên")
    safe_print(f"   - Max products mỗi category: {max_products_per_category}")
    safe_print(f"   - Timeout: 60s mỗi request")
    safe_print("")
    
    # Chỉ crawl một vài categories để demo nhanh
    demo_categories = categories[:max_categories]
    
    all_products = crawl_products_from_categories(
        categories=demo_categories,
        max_products_per_category=max_products_per_category,
        max_categories=max_categories,
        timeout=60
    )
    
    # Lưu cache
    if all_products:
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        save_products_to_json(all_products, cache_file)
        safe_print(f"\n💾 Đã lưu cache vào: {cache_file}")
    
    safe_print(f"\n✓ Tổng cộng crawl được {len(all_products)} products")
    
    return all_products


def demo_display_products(products):
    """Display products"""
    print_section("BƯỚC 3: Hiển Thị Products")
    
    if not products:
        safe_print("⚠️  Không có products để hiển thị")
        return
    
    safe_print(f"📊 Tổng cộng: {len(products)} products")
    
    # Group by category
    by_category = {}
    for product in products:
        cat_name = product.get('category_name', 'Unknown')
        if cat_name not in by_category:
            by_category[cat_name] = []
        by_category[cat_name].append(product)
    
    safe_print(f"\n📋 Phân bố theo category:")
    for cat_name, prods in by_category.items():
        safe_print(f"  - {cat_name}: {len(prods)} products")
    
    # Hiển thị sample products
    safe_print("\n📦 Sample Products (first 5):")
    safe_print("-" * 70)
    
    for i, product in enumerate(products[:5], 1):
        print_product_info(product)
        if i < min(5, len(products)):
            safe_print()
    
    if len(products) > 5:
        safe_print(f"\n... và {len(products) - 5} products khác")


def demo_save_products(products):
    """Save products to file"""
    print_section("BƯỚC 4: Lưu Products")
    
    output_dir = "data/raw/demo"
    os.makedirs(output_dir, exist_ok=True)
    
    if not products:
        safe_print("⚠️  Không có products để lưu")
        return []
    
    # Lưu products
    products_file = os.path.join(output_dir, "demo_products.json")
    save_products_to_json(products, products_file)
    
    # Tạo summary
    summary = {
        'crawl_time': datetime.now().isoformat(),
        'total_products': len(products),
        'by_category': {},
        'files_saved': [products_file]
    }
    
    # Group by category
    for product in products:
        cat_name = product.get('category_name', 'Unknown')
        if cat_name not in summary['by_category']:
            summary['by_category'][cat_name] = 0
        summary['by_category'][cat_name] += 1
    
    summary_file = os.path.join(output_dir, "demo_products_summary.json")
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    safe_print(f"✓ Đã lưu summary vào: {summary_file}")
    
    return [products_file, summary_file]


def main():
    """Chạy demo quy trình crawl products"""
    safe_print("\n" + "=" * 70)
    safe_print(" " * 20 + "TIKI PRODUCTS CRAWLER - DEMO")
    safe_print("=" * 70)
    safe_print(f"Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    safe_print("\n💡 Đây là demo với dữ liệu nhỏ để chạy nhanh")
    safe_print("   - Chỉ crawl từ 2 categories đầu tiên")
    safe_print("   - Max 5 products mỗi category")
    safe_print("   - Sử dụng cache để tránh crawl lại")
    
    # ============================================
    # CẤU HÌNH - Điều chỉnh để chạy nhanh/chậm
    # ============================================
    USE_CACHE = True          # Sử dụng cache nếu có (True = nhanh hơn)
    MAX_CATEGORIES = 2        # Chỉ crawl 2 categories (giảm để nhanh)
    MAX_PRODUCTS_PER_CATEGORY = 5  # Max 5 products/category
    # ============================================
    
    try:
        # Bước 1: Load categories
        categories = demo_load_categories()
        
        if not categories:
            safe_print("\n⚠️  Không thể load categories, dừng demo")
            return
        
        # Bước 2: Crawl products
        products = demo_crawl_products(
            categories,
            use_cache=USE_CACHE,
            max_categories=MAX_CATEGORIES,
            max_products_per_category=MAX_PRODUCTS_PER_CATEGORY
        )
        
        # Bước 3: Display
        demo_display_products(products)
        
        # Bước 4: Save
        files_saved = demo_save_products(products)
        
        # Summary
        print_section("TÓM TẮT")
        
        safe_print(f"✓ Đã crawl thành công:")
        safe_print(f"  - {len(categories)} categories")
        safe_print(f"  - {len(products)} products")
        
        safe_print(f"\n✓ Đã lưu {len(files_saved)} files vào thư mục: data/raw/demo/")
        
        safe_print("\n📁 Các file đã lưu:")
        for file in files_saved:
            safe_print(f"  - {file}")
        
        safe_print("\n💡 Bạn có thể mở file JSON để xem chi tiết products")
        
        safe_print("\n" + "=" * 70)
        safe_print("✅ Demo hoàn thành!")
        safe_print("=" * 70)
        
    except KeyboardInterrupt:
        safe_print("\n\n⚠️  Đã dừng bởi người dùng")
    except Exception as e:
        safe_print(f"\n\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

