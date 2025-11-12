"""
Test script end-to-end cho Tiki Crawler Pipeline
Test luồng crawl từ categories -> sub-categories -> products (nếu có)
"""
import os
import sys
import json
import requests
from datetime import datetime

# Thêm path để import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../src'))

from pipelines.crawl.tiki.extract_category_link import (
    load_categories_from_json,
    crawl_sub_categories,
    build_hierarchical_structure,
    create_merged_categories_file
)
from pipelines.crawl.tiki.config import get_config

# Fix encoding on Windows - chỉ fix nếu chưa được fix
if sys.platform == "win32":
    try:
        # Kiểm tra xem stdout đã được wrap chưa
        if not hasattr(sys.stdout, 'buffer') or sys.stdout.encoding != 'utf-8':
            import io
            # Chỉ wrap nếu chưa được wrap
            if not isinstance(sys.stdout, io.TextIOWrapper) or sys.stdout.encoding != 'utf-8':
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        # Nếu không thể wrap, bỏ qua
        pass

config = get_config()
FIRECRAWL_API_URL = os.getenv("FIRECRAWL_API_URL", "http://localhost:3002")


def test_firecrawl_connection():
    """Test kết nối với Firecrawl API"""
    print("=" * 60)
    print("TEST 1: Kiểm tra kết nối Firecrawl API")
    print("=" * 60)
    
    try:
        # Test health check hoặc simple scrape
        test_url = "https://tiki.vn"
        payload = {
            "url": test_url,
            "onlyMainContent": True,
            "maxAge": 172800000,
            "formats": ["html"]
        }
        
        print(f"Đang test crawl: {test_url}")
        response = requests.post(
            f"{FIRECRAWL_API_URL}/v2/scrape",
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        data = response.json()
        print(f"✓ Kết nối Firecrawl thành công")
        print(f"  - Status: {response.status_code}")
        print(f"  - Có HTML: {'html' in data.get('data', {})}")
        print(f"  - Có Markdown: {'markdown' in data.get('data', {})}")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Lỗi kết nối Firecrawl: {e}")
        print(f"  Kiểm tra xem Firecrawl API có đang chạy tại {FIRECRAWL_API_URL} không")
        return False
    except Exception as e:
        print(f"✗ Lỗi: {e}")
        return False


def test_crawl_single_category():
    """Test crawl sub-categories từ 1 category"""
    print("\n" + "=" * 60)
    print("TEST 2: Crawl sub-categories từ 1 category")
    print("=" * 60)
    
    # Chọn 1 category để test (ví dụ: Thời trang nam)
    test_category = {
        'name': 'Thời trang nam',
        'url': 'https://tiki.vn/thoi-trang-nam/c915',
        'category_id': '915',
        'slug': 'c915'
    }
    
    print(f"Đang crawl sub-categories từ: {test_category['name']}")
    print(f"URL: {test_category['url']}")
    
    try:
        sub_categories = crawl_sub_categories(
            category_url=test_category['url'],
            parent_category_id=test_category['category_id'],
            parent_name=test_category['name']
        )
        
        if sub_categories:
            print(f"✓ Tìm thấy {len(sub_categories)} sub-categories")
            print("\nMột vài sub-categories đầu tiên:")
            for i, sub_cat in enumerate(sub_categories[:5], 1):
                print(f"  {i}. {sub_cat.get('name', 'N/A')} (ID: {sub_cat.get('category_id', 'N/A')})")
            
            # Lưu vào file test
            test_output = "data/raw/test_sub_categories.json"
            os.makedirs(os.path.dirname(test_output), exist_ok=True)
            with open(test_output, 'w', encoding='utf-8') as f:
                json.dump(sub_categories, f, indent=2, ensure_ascii=False)
            print(f"\n✓ Đã lưu vào: {test_output}")
            return True
        else:
            print("⚠️  Không tìm thấy sub-categories")
            return False
            
    except Exception as e:
        print(f"✗ Lỗi khi crawl: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_load_and_merge_categories():
    """Test load categories và tạo file merged"""
    print("\n" + "=" * 60)
    print("TEST 3: Load categories và tạo file merged")
    print("=" * 60)
    
    categories_file = config['data_paths']['all_categories']
    
    if not os.path.exists(categories_file):
        print(f"⚠️  File {categories_file} không tồn tại")
        print("   Chạy crawl categories trước")
        return False
    
    print(f"Đang load từ: {categories_file}")
    
    try:
        with open(categories_file, 'r', encoding='utf-8') as f:
            categories = json.load(f)
        
        print(f"✓ Đã load {len(categories)} categories")
        
        # Test build hierarchical structure với sample nhỏ
        print("\nĐang test build hierarchical structure với 50 categories đầu tiên...")
        sample_categories = categories[:50]
        hierarchical = build_hierarchical_structure(sample_categories)
        
        print(f"✓ Đã tạo cấu trúc phân cấp với {len(hierarchical)} root categories")
        
        # Đếm tổng số categories trong cấu trúc phân cấp
        def count_categories(cats, level=1):
            total = len(cats)
            for cat in cats:
                if 'sub_categories' in cat and cat['sub_categories']:
                    total += count_categories(cat['sub_categories'], level + 1)
            return total
        
        total = count_categories(hierarchical)
        print(f"  - Tổng categories (bao gồm sub): {total}")
        
        # Lưu sample merged
        test_merged = "data/raw/test_merged_categories.json"
        with open(test_merged, 'w', encoding='utf-8') as f:
            json.dump(hierarchical, f, indent=2, ensure_ascii=False)
        print(f"✓ Đã lưu sample merged vào: {test_merged}")
        
        return True
        
    except Exception as e:
        print(f"✗ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_crawl_products_sample():
    """Test crawl products từ 1 category nhỏ"""
    print("\n" + "=" * 60)
    print("TEST 4: Crawl products từ 1 category (sample)")
    print("=" * 60)
    
    # Chọn 1 category nhỏ để test (ví dụ: Áo thun nam)
    test_category = {
        'name': 'Áo thun nam',
        'url': 'https://tiki.vn/ao-thun-nam/c917',
        'category_id': '917'
    }
    
    print(f"Đang crawl products từ: {test_category['name']}")
    print(f"URL: {test_category['url']}")
    print("(Lưu ý: Đây là test cơ bản, cần implement parsing logic)")
    
    try:
        payload = {
            "url": test_category['url'],
            "onlyMainContent": True,
            "maxAge": 172800000,
            "formats": ["html", "markdown"]
        }
        
        response = requests.post(
            f"{FIRECRAWL_API_URL}/v2/scrape",
            json=payload,
            timeout=60
        )
        response.raise_for_status()
        
        data = response.json()
        
        # Kiểm tra response
        has_html = 'html' in data.get('data', {})
        has_markdown = 'markdown' in data.get('data', {})
        
        print(f"✓ Crawl thành công")
        print(f"  - Có HTML: {has_html}")
        print(f"  - Có Markdown: {has_markdown}")
        
        if has_html:
            html_length = len(data['data'].get('html', ''))
            print(f"  - HTML length: {html_length} characters")
        
        if has_markdown:
            markdown_length = len(data['data'].get('markdown', ''))
            print(f"  - Markdown length: {markdown_length} characters")
            # Hiển thị một phần markdown
            markdown_preview = data['data'].get('markdown', '')[:500]
            print(f"\n  Markdown preview (500 chars đầu):")
            print(f"  {markdown_preview}...")
        
        # Lưu raw response để phân tích
        test_output = "data/raw/test_product_crawl.json"
        os.makedirs(os.path.dirname(test_output), exist_ok=True)
        with open(test_output, 'w', encoding='utf-8') as f:
            json.dump({
                'category': test_category,
                'crawl_time': datetime.now().isoformat(),
                'response': data
            }, f, indent=2, ensure_ascii=False)
        print(f"\n✓ Đã lưu raw response vào: {test_output}")
        print("  (Có thể dùng file này để develop parsing logic)")
        
        return True
        
    except Exception as e:
        print(f"✗ Lỗi khi crawl products: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_data_structure():
    """Test cấu trúc dữ liệu đã crawl"""
    print("\n" + "=" * 60)
    print("TEST 5: Kiểm tra cấu trúc dữ liệu")
    print("=" * 60)
    
    files_to_check = [
        ('categories', config['data_paths']['categories']),
        ('sub_categories', config['data_paths']['sub_categories']),
        ('all_categories', config['data_paths']['all_categories']),
        ('merged', config['data_paths']['merged_categories']),
    ]
    
    results = {}
    
    for name, filepath in files_to_check:
        if os.path.exists(filepath):
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                if isinstance(data, list):
                    count = len(data)
                    print(f"✓ {name}: {count} items")
                    results[name] = count
                    
                    # Hiển thị sample nếu có
                    if count > 0:
                        sample = data[0]
                        print(f"  Sample keys: {list(sample.keys())[:5]}")
                else:
                    print(f"✓ {name}: {type(data).__name__}")
                    results[name] = 'exists'
            except Exception as e:
                print(f"✗ {name}: Lỗi khi đọc - {e}")
                results[name] = 'error'
        else:
            print(f"⚠️  {name}: File không tồn tại")
            results[name] = 'missing'
    
    return results


def main():
    """Chạy tất cả tests"""
    try:
        print("\n" + "=" * 60)
        print("TIKI CRAWLER - END-TO-END TEST")
        print("=" * 60)
        print(f"Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Firecrawl API: {FIRECRAWL_API_URL}")
        print()
    except (ValueError, OSError) as e:
        # Nếu có lỗi với stdout, thử fix lại
        if sys.platform == "win32":
            import io
            try:
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
                print("\n" + "=" * 60)
                print("TIKI CRAWLER - END-TO-END TEST")
                print("=" * 60)
                print(f"Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"Firecrawl API: {FIRECRAWL_API_URL}")
                print()
            except:
                # Nếu vẫn lỗi, dùng stderr
                sys.stderr.write("\n" + "=" * 60 + "\n")
                sys.stderr.write("TIKI CRAWLER - END-TO-END TEST\n")
                sys.stderr.write("=" * 60 + "\n")
                sys.stderr.write(f"Thời gian: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                sys.stderr.write(f"Firecrawl API: {FIRECRAWL_API_URL}\n\n")
                sys.stderr.flush()
    
    test_results = {}
    
    # Test 1: Connection
    test_results['connection'] = test_firecrawl_connection()
    
    if not test_results['connection']:
        print("\n⚠️  Không thể kết nối Firecrawl, bỏ qua các tests còn lại")
        return test_results
    
    # Test 2: Crawl single category
    test_results['crawl_category'] = test_crawl_single_category()
    
    # Test 3: Load and merge
    test_results['load_merge'] = test_load_and_merge_categories()
    
    # Test 4: Crawl products (sample)
    test_results['crawl_products'] = test_crawl_products_sample()
    
    # Test 5: Data structure
    test_results['data_structure'] = test_data_structure()
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for test_name, result in test_results.items():
        if isinstance(result, bool):
            status = "✓ PASS" if result else "✗ FAIL"
        else:
            status = f"✓ {result}"
        print(f"{test_name:20s}: {status}")
    
    passed = sum(1 for r in test_results.values() if r is True)
    total = sum(1 for r in test_results.values() if isinstance(r, bool))
    
    print(f"\nKết quả: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 Tất cả tests đều PASS!")
    else:
        print("⚠️  Một số tests FAIL, kiểm tra logs ở trên")
    
    return test_results


def safe_print(*args, **kwargs):
    """Print an toàn, fallback sang stderr nếu stdout bị lỗi"""
    try:
        print(*args, **kwargs)
    except (ValueError, OSError):
        try:
            sys.stderr.write(' '.join(str(arg) for arg in args) + '\n')
            sys.stderr.flush()
        except:
            pass


if __name__ == "__main__":
    try:
        results = main()
        exit_code = 0 if all(r is True or not isinstance(r, bool) for r in results.values()) else 1
        sys.exit(exit_code)
    except KeyboardInterrupt:
        safe_print("\n\n⚠️  Test bị hủy bởi user")
        sys.exit(1)
    except Exception as e:
        safe_print(f"\n\n✗ Lỗi không mong đợi: {e}")
        try:
            import traceback
            traceback.print_exc()
        except:
            pass
        sys.exit(1)

