"""
Demo script để test quy trình crawl Tiki với dữ liệu nhỏ
Chạy nhanh để xem toàn bộ quy trình và dữ liệu
"""
import os
import sys
import json
import requests
from datetime import datetime

# Thêm path để import modules
# Tính toán đường dẫn tuyệt đối đến src từ script hiện tại
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.abspath(os.path.join(_script_dir, '..', '..'))
_src_path = os.path.join(_project_root, 'src')
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from pipelines.crawl.tiki.extract_category_link import (
    parse_firecrawl_response,
    crawl_sub_categories,
    crawl_categories_recursive,
    build_hierarchical_structure,
    validate_hierarchical_structure
)
from pipelines.crawl.tiki.config import get_config

# Fix encoding on Windows
if sys.platform == "win32":
    try:
        if not hasattr(sys.stdout, 'buffer') or (hasattr(sys.stdout, 'encoding') and sys.stdout.encoding != 'utf-8'):
            import io
            if not isinstance(sys.stdout, io.TextIOWrapper):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass

config = get_config()
FIRECRAWL_API_URL = os.getenv("FIRECRAWL_API_URL", "http://localhost:3002")
TIKI_BASE_URL = "https://tiki.vn"


def print_section(title):
    """In tiêu đề section"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_category_info(cat, indent=0):
    """In thông tin category với indent"""
    prefix = "  " * indent
    print(f"{prefix}├─ {cat.get('name', 'N/A')}")
    print(f"{prefix}│  ID: {cat.get('category_id', 'N/A')}")
    print(f"{prefix}│  URL: {cat.get('url', 'N/A')}")
    if cat.get('parent_name'):
        print(f"{prefix}│  Parent: {cat.get('parent_name')} (ID: {cat.get('parent_id')})")


def demo_crawl_categories(use_cache=True):
    """Demo crawl categories từ trang chủ Tiki hoặc load từ cache"""
    print_section("BƯỚC 1: Crawl Categories từ Trang Chủ Tiki")
    
    # Kiểm tra cache
    cache_file = "data/raw/demo/demo_categories_cache.json"
    if use_cache and os.path.exists(cache_file):
        print("💡 Tìm thấy cache, đang load từ cache (nhanh hơn)...")
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_categories = json.load(f)
            print(f"✓ Đã load {len(cached_categories)} categories từ cache")
            
            # Chỉ lấy 3 categories đầu để demo nhanh
            demo_categories = cached_categories[:3]
            print(f"   (Sử dụng {len(demo_categories)} categories đầu để demo)")
            return demo_categories
        except Exception as e:
            print(f"⚠️  Lỗi khi load cache: {e}, sẽ crawl mới...")
    
    print(f"Đang crawl từ: {TIKI_BASE_URL}")
    print(f"Firecrawl API: {FIRECRAWL_API_URL}")
    
    payload = {
        "url": TIKI_BASE_URL,
        "onlyMainContent": True,
        "maxAge": 172800000,
        "formats": ["html"]
    }
    
    try:
        print("\n⏳ Đang gọi Firecrawl API...")
        response = requests.post(
            f"{FIRECRAWL_API_URL}/v2/scrape",
            json=payload,
            timeout=30  # Giảm timeout
        )
        response.raise_for_status()
        
        data = response.json()
        print("✓ Crawl thành công!")
        
        print("\n📊 Đang parse và extract categories...")
        categories = parse_firecrawl_response(data)
        
        # Lưu cache
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(categories, f, indent=2, ensure_ascii=False)
        print(f"💾 Đã lưu cache vào: {cache_file}")
        
        # Chỉ lấy 3 categories đầu để demo nhanh
        demo_categories = categories[:3]
        
        print(f"\n✓ Tìm thấy {len(categories)} categories (sử dụng {len(demo_categories)} đầu để demo):")
        print("-" * 70)
        
        for i, cat in enumerate(demo_categories, 1):
            print(f"{i}. {cat.get('name', 'N/A')} (ID: {cat.get('category_id', 'N/A')})")
        
        return demo_categories
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Lỗi kết nối: {e}")
        print("  Kiểm tra xem Firecrawl API có đang chạy không")
        return []
    except Exception as e:
        print(f"✗ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        return []


def demo_crawl_sub_categories(categories, recursive=True, max_depth=2, use_cache=True, max_categories=1):
    """
    Demo crawl sub-categories từ một vài categories
    Sử dụng crawl đệ quy để crawl tất cả các level
    """
    print_section("BƯỚC 2: Crawl Sub-Categories (Đệ Quy)")
    
    if not categories:
        print("⚠️  Không có categories để crawl sub-categories")
        return []
    
    # Chỉ crawl 1 category đầu để demo nhanh
    demo_count = min(max_categories, len(categories))
    demo_categories = categories[:demo_count]
    
    print(f"Đang crawl sub-categories từ {demo_count} category đầu tiên...")
    print(f"Mode: {'Đệ quy (tất cả các level)' if recursive else 'Chỉ 1 level'}")
    if recursive:
        print(f"Max depth: {max_depth if max_depth else 'unlimited'}")
        print(f"💡 Giảm max_depth và số lượng để chạy nhanh hơn")
    
    # Kiểm tra cache
    cache_file = f"data/raw/demo/demo_sub_categories_cache_{demo_categories[0].get('category_id', 'unknown')}.json"
    if use_cache and os.path.exists(cache_file):
        print(f"\n💡 Tìm thấy cache, đang load từ cache...")
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                cached_sub_categories = json.load(f)
            print(f"✓ Đã load {len(cached_sub_categories)} sub-categories từ cache")
            return cached_sub_categories
        except Exception as e:
            print(f"⚠️  Lỗi khi load cache: {e}, sẽ crawl mới...")
    
    if recursive:
        # Sử dụng crawl đệ quy với giới hạn chặt chẽ để nhanh
        print("\n💡 Sử dụng crawl đệ quy với giới hạn để chạy nhanh")
        print("   - Max depth: 2 (có thể tăng nếu cần)")
        print("   - Chỉ crawl 1 category đầu")
        print("   - Tự động dừng khi không còn sub-categories\n")
        
        stats = {
            'total_crawled': 0,
            'total_found': 0,
            'by_level': {},
            'errors': 0
        }
        
        all_sub_categories = crawl_categories_recursive(
            demo_categories,
            visited_ids=set(),
            max_depth=max_depth,  # Giảm xuống 2 để nhanh
            current_depth=0,
            max_categories_per_level=5,  # Giới hạn 5 categories/level
            stats=stats
        )
        
        # Lưu cache
        os.makedirs(os.path.dirname(cache_file), exist_ok=True)
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(all_sub_categories, f, indent=2, ensure_ascii=False)
        print(f"💾 Đã lưu cache vào: {cache_file}")
        
        print(f"\n✓ Thống kê crawl:")
        print(f"   - Tổng categories đã crawl: {stats['total_crawled']}")
        print(f"   - Tổng sub-categories tìm thấy: {stats['total_found']}")
        print(f"   - Unique sub-categories: {len(all_sub_categories)}")
        print(f"   - Lỗi: {stats['errors']}")
        if stats['by_level']:
            print(f"   - Phân bố theo level:")
            for level, count in sorted(stats['by_level'].items()):
                print(f"     {level}: {count} categories")
    else:
        # Chỉ crawl 1 level
        all_sub_categories = []
        
        for i, cat in enumerate(demo_categories, 1):
            cat_name = cat.get('name', 'N/A')
            cat_url = cat.get('url', '')
            cat_id = cat.get('category_id', '')
            
            print(f"\n[{i}/{demo_count}] 📂 Category: {cat_name}")
            print(f"   URL: {cat_url}")
            print("   ⏳ Đang crawl sub-categories...")
            
            try:
                sub_cats = crawl_sub_categories(
                    category_url=cat_url,
                    parent_category_id=cat_id,
                    parent_name=cat_name
                )
                
                if sub_cats:
                    # Chỉ lấy 3 sub-categories đầu để hiển thị
                    display_cats = sub_cats[:3]
                    print(f"   ✓ Tìm thấy {len(sub_cats)} sub-categories (hiển thị {len(display_cats)} đầu):")
                    
                    for j, sub_cat in enumerate(display_cats, 1):
                        print(f"      {j}. {sub_cat.get('name', 'N/A')} (ID: {sub_cat.get('category_id', 'N/A')})")
                    
                    if len(sub_cats) > 3:
                        print(f"      ... và {len(sub_cats) - 3} sub-categories khác")
                    
                    all_sub_categories.extend(sub_cats)
                else:
                    print(f"   - Không tìm thấy sub-categories")
                    
            except Exception as e:
                print(f"   ✗ Lỗi: {e}")
                continue
        
        print(f"\n✓ Tổng cộng crawl được {len(all_sub_categories)} sub-categories")
    
    return all_sub_categories


def demo_build_hierarchical(categories, sub_categories):
    """Demo build cấu trúc phân cấp"""
    print_section("BƯỚC 3: Xây Dựng Cấu Trúc Phân Cấp")
    
    # Kết hợp categories và sub_categories
    all_cats = []
    
    # Thêm parent categories
    for cat in categories:
        cat_copy = cat.copy()
        cat_copy['parent_id'] = None
        cat_copy['parent_name'] = None
        cat_copy['parent_url'] = None
        all_cats.append(cat_copy)
    
    # Thêm sub-categories
    all_cats.extend(sub_categories)
    
    print(f"Đang xây dựng cấu trúc phân cấp từ {len(all_cats)} categories...")
    
    try:
        hierarchical = build_hierarchical_structure(all_cats)
        
        print(f"✓ Đã tạo cấu trúc phân cấp với {len(hierarchical)} root categories")
        
        # Validate cấu trúc
        print("\n🔍 Validating cấu trúc phân cấp...")
        validation_result = validate_hierarchical_structure(hierarchical, all_cats)
        
        print(f"✓ Validation Result: {validation_result['is_valid']}")
        print(f"  - Collected: {validation_result['stats']['total_collected']}/{validation_result['stats']['total_original']}")
        print(f"  - Missing: {validation_result['stats']['total_missing']}")
        print(f"  - Max Depth: {validation_result['stats']['max_depth']}")
        
        if validation_result['errors']:
            print(f"\n⚠️  {len(validation_result['errors'])} issues found:")
            for error in validation_result['errors'][:5]:
                print(f"  - {error}")
            if len(validation_result['errors']) > 5:
                print(f"  ... and {len(validation_result['errors']) - 5} more")
        
        # Hiển thị cấu trúc
        print("\n📊 Cấu trúc phân cấp (sample):")
        print("-" * 70)
        
        def print_tree(cats, indent=0, max_depth=2, current_depth=0):
            """In cây categories với giới hạn độ sâu"""
            if current_depth >= max_depth:
                return
            
            for cat in cats[:3]:  # Chỉ hiển thị 3 đầu tiên
                print_category_info(cat, indent)
                
                if 'sub_categories' in cat and cat['sub_categories']:
                    sub_count = len(cat['sub_categories'])
                    if sub_count > 0:
                        print(f"{'  ' * indent}│  Sub-categories: {sub_count}")
                        # Chỉ hiển thị 2 sub đầu tiên
                        print_tree(cat['sub_categories'][:2], indent + 1, max_depth, current_depth + 1)
                
                if cat != cats[-1] if len(cats) > 1 else True:
                    print()
            
            if len(cats) > 3:
                print(f"{'  ' * indent}└─ ... và {len(cats) - 3} categories khác")
        
        print_tree(hierarchical)
        
        return hierarchical
        
    except Exception as e:
        print(f"✗ Lỗi khi build cấu trúc: {e}")
        import traceback
        traceback.print_exc()
        return []


def demo_save_data(categories, sub_categories, hierarchical):
    """Demo lưu dữ liệu"""
    print_section("BƯỚC 4: Lưu Dữ Liệu")
    
    output_dir = "data/raw/demo"
    os.makedirs(output_dir, exist_ok=True)
    
    files_saved = []
    
    # Lưu categories
    if categories:
        categories_file = os.path.join(output_dir, "demo_categories.json")
        with open(categories_file, 'w', encoding='utf-8') as f:
            json.dump(categories, f, indent=2, ensure_ascii=False)
        files_saved.append(categories_file)
        print(f"✓ Đã lưu {len(categories)} categories vào: {categories_file}")
    
    # Lưu sub-categories
    if sub_categories:
        sub_categories_file = os.path.join(output_dir, "demo_sub_categories.json")
        with open(sub_categories_file, 'w', encoding='utf-8') as f:
            json.dump(sub_categories, f, indent=2, ensure_ascii=False)
        files_saved.append(sub_categories_file)
        print(f"✓ Đã lưu {len(sub_categories)} sub-categories vào: {sub_categories_file}")
    
    # Lưu hierarchical structure
    if hierarchical:
        hierarchical_file = os.path.join(output_dir, "demo_hierarchical.json")
        with open(hierarchical_file, 'w', encoding='utf-8') as f:
            json.dump(hierarchical, f, indent=2, ensure_ascii=False)
        files_saved.append(hierarchical_file)
        print(f"✓ Đã lưu cấu trúc phân cấp vào: {hierarchical_file}")
    
    # Tạo summary
    summary = {
        'crawl_time': datetime.now().isoformat(),
        'total_categories': len(categories),
        'total_sub_categories': len(sub_categories),
        'total_root_categories': len(hierarchical) if hierarchical else 0,
        'files_saved': files_saved
    }
    
    summary_file = os.path.join(output_dir, "demo_summary.json")
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
    
    print(f"\n✓ Đã lưu summary vào: {summary_file}")
    
    return files_saved


def main():
    """Chạy demo quy trình crawl"""
    print("\n" + "=" * 70)
    print(" " * 20 + "TIKI CRAWLER - DEMO")
    print("=" * 70)
    print(f"Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Firecrawl API: {FIRECRAWL_API_URL}")
    print("\n💡 Đây là demo với dữ liệu nhỏ để chạy nhanh")
    print("   - Chỉ crawl 3 categories đầu tiên (hoặc từ cache)")
    print("   - Chỉ crawl sub-categories từ 1 category đầu")
    print("   - Max depth: 2 (giảm để chạy nhanh)")
    print("   - Sử dụng cache để tránh crawl lại")
    print("   - Hiển thị sample data để xem cấu trúc")
    
    # ============================================
    # CẤU HÌNH - Điều chỉnh để chạy nhanh/chậm
    # ============================================
    USE_CACHE = True          # Sử dụng cache nếu có (True = nhanh hơn)
    SKIP_CRAWL = False        # True = chỉ hiển thị dữ liệu đã có, không crawl
    MAX_CATEGORIES = 1        # Chỉ crawl 1 category (giảm để nhanh)
    MAX_DEPTH = 5            # Chỉ crawl 2 levels (giảm để nhanh)
    MAX_CATEGORIES_PER_LEVEL = 5  # Giới hạn 5 categories/level
    RECURSIVE = True          # Bật crawl đệ quy
    SKIP_BUILD_HIERARCHICAL = False  # True = bỏ qua bước build hierarchical
    # ============================================
    
    if SKIP_CRAWL:
        print("\n⚠️  SKIP_CRAWL=True: Chỉ hiển thị dữ liệu đã có, không crawl mới")
    
    try:
        categories = []
        sub_categories = []
        hierarchical = []
        
        if not SKIP_CRAWL:
            # Bước 1: Crawl categories (hoặc load từ cache)
            categories = demo_crawl_categories(use_cache=USE_CACHE)
            
            if not categories:
                print("\n⚠️  Không thể crawl categories, dừng demo")
                return
            
            # Bước 2: Crawl sub-categories (đệ quy với giới hạn)
            sub_categories = demo_crawl_sub_categories(
                categories, 
                recursive=RECURSIVE,
                max_depth=MAX_DEPTH,      # Chỉ 2 levels để nhanh
                use_cache=USE_CACHE,      # Sử dụng cache
                max_categories=MAX_CATEGORIES  # Chỉ 1 category
            )
        else:
            # Chỉ load từ file đã có
            print_section("LOAD DỮ LIỆU ĐÃ CÓ (Không Crawl)")
            
            # Load categories
            categories_file = "data/raw/demo/demo_categories.json"
            if os.path.exists(categories_file):
                with open(categories_file, 'r', encoding='utf-8') as f:
                    categories = json.load(f)
                print(f"✓ Đã load {len(categories)} categories từ file")
            else:
                print(f"⚠️  File {categories_file} không tồn tại")
            
            # Load sub-categories
            sub_categories_file = "data/raw/demo/demo_sub_categories.json"
            if os.path.exists(sub_categories_file):
                with open(sub_categories_file, 'r', encoding='utf-8') as f:
                    sub_categories = json.load(f)
                print(f"✓ Đã load {len(sub_categories)} sub-categories từ file")
            else:
                print(f"⚠️  File {sub_categories_file} không tồn tại")
        
        # Bước 3: Build hierarchical structure (có thể skip)
        if not SKIP_BUILD_HIERARCHICAL:
            hierarchical = demo_build_hierarchical(categories, sub_categories)
        else:
            print_section("SKIP: Xây Dựng Cấu Trúc Phân Cấp")
            print("⚠️  Đã bỏ qua bước này để chạy nhanh hơn")
            hierarchical = []
        
        # Bước 4: Lưu dữ liệu
        files_saved = demo_save_data(categories, sub_categories, hierarchical)
        
        # Summary
        print_section("TÓM TẮT")
        
        print(f"✓ Đã crawl thành công:")
        print(f"  - {len(categories)} categories")
        print(f"  - {len(sub_categories)} sub-categories")
        print(f"  - {len(hierarchical)} root categories trong cấu trúc phân cấp")
        
        print(f"\n✓ Đã lưu {len(files_saved)} files vào thư mục: data/raw/demo/")
        print("\n📁 Các file đã lưu:")
        for file in files_saved:
            print(f"  - {file}")
        
        print("\n💡 Bạn có thể mở các file JSON để xem chi tiết dữ liệu")
        print("   Hoặc chạy script extract_category_link.py để crawl đầy đủ")
        
        print("\n" + "=" * 70)
        print("✅ Demo hoàn thành!")
        print("=" * 70)
        
    except KeyboardInterrupt:
        print("\n\n⚠️  Demo bị hủy bởi user")
    except Exception as e:
        print(f"\n\n✗ Lỗi không mong đợi: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()

