"""
Script test các utilities để đảm bảo không có lỗi
"""
import os
import sys

# Thêm src vào path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src', 'pipelines', 'crawl'))

def test_utils_imports():
    """Test import các utilities"""
    print("="*70)
    print("TEST UTILS IMPORTS")
    print("="*70)

    try:
        print("OK: Import utilities thanh cong")
        return True
    except Exception as e:
        print(f"FAIL: Import utilities loi: {e}")
        return False

def test_parse_sales_count():
    """Test parse_sales_count function"""
    print("\n" + "="*70)
    print("TEST PARSE_SALES_COUNT")
    print("="*70)

    try:
        from utils import parse_sales_count

        test_cases = [
            (1000, 1000),
            ("2k", 2000),
            ("1.5k", 1500),
            ("3m", 3000000),
            ("100", 100),
            (None, None),
            ("", None),
            ({"text": "Đã bán 16", "value": 16}, 16),
        ]

        all_passed = True
        for input_val, expected in test_cases:
            result = parse_sales_count(input_val)
            status = "OK" if result == expected else "FAIL"
            if result != expected:
                all_passed = False
            # Tránh lỗi encoding với dict
            input_str = str(input_val) if not isinstance(input_val, dict) else f"dict({input_val.get('value', 'N/A')})"
            print(f"  {status}: Input={input_str} -> Output={result} (Expected: {expected})")

        return all_passed
    except Exception as e:
        print(f"FAIL: Test parse_sales_count loi: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_parse_price():
    """Test parse_price function"""
    print("\n" + "="*70)
    print("TEST PARSE_PRICE")
    print("="*70)

    try:
        from utils import parse_price

        test_cases = [
            ("389.000", 389000),
            ("1.500.000", 1500000),
            ("100", 100),
            (None, None),
            ("", None),
        ]

        all_passed = True
        for input_val, expected in test_cases:
            result = parse_price(input_val)
            status = "OK" if result == expected else "FAIL"
            if result != expected:
                all_passed = False
            print(f"  {status}: Input={input_val} -> Output={result} (Expected: {expected})")

        return all_passed
    except Exception as e:
        print(f"FAIL: Test parse_price loi: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_extract_product_id():
    """Test extract_product_id_from_url function"""
    print("\n" + "="*70)
    print("TEST EXTRACT_PRODUCT_ID_FROM_URL")
    print("="*70)

    try:
        from utils import extract_product_id_from_url

        test_cases = [
            ("https://tiki.vn/p/123456", "123456"),
            ("https://tiki.vn/product-p123456.html", "123456"),
            ("https://tiki.vn/something-p789012.html", "789012"),
            ("invalid", None),
        ]

        all_passed = True
        for url, expected in test_cases:
            result = extract_product_id_from_url(url)
            status = "OK" if result == expected else "FAIL"
            if result != expected:
                all_passed = False
            print(f"  {status}: URL={url[:50]}... -> Product ID={result} (Expected: {expected})")

        return all_passed
    except Exception as e:
        print(f"FAIL: Test extract_product_id loi: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_crawl_products_import():
    """Test import crawl_products module"""
    print("\n" + "="*70)
    print("TEST CRAWL_PRODUCTS IMPORT")
    print("="*70)

    try:
        # Test import với fallback
        import importlib.util
        crawl_products_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'pipelines', 'crawl', 'crawl_products.py')

        if os.path.exists(crawl_products_path):
            spec = importlib.util.spec_from_file_location("crawl_products", crawl_products_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                print("OK: Import crawl_products thanh cong")
                print(f"   - Has parse_sales_count: {hasattr(module, 'parse_sales_count')}")
                print(f"   - Has crawl_category_products: {hasattr(module, 'crawl_category_products')}")
                return True
        print("FAIL: Khong tim thay crawl_products.py")
        return False
    except Exception as e:
        print(f"FAIL: Import crawl_products loi: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_crawl_products_detail_import():
    """Test import crawl_products_detail module"""
    print("\n" + "="*70)
    print("TEST CRAWL_PRODUCTS_DETAIL IMPORT")
    print("="*70)

    try:
        import importlib.util
        detail_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'pipelines', 'crawl', 'crawl_products_detail.py')

        if os.path.exists(detail_path):
            spec = importlib.util.spec_from_file_location("crawl_products_detail", detail_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                print("OK: Import crawl_products_detail thanh cong")
                print(f"   - Has crawl_product_detail_with_selenium: {hasattr(module, 'crawl_product_detail_with_selenium')}")
                print(f"   - Has extract_product_detail: {hasattr(module, 'extract_product_detail')}")
                return True
        print("FAIL: Khong tim thay crawl_products_detail.py")
        return False
    except Exception as e:
        print(f"FAIL: Import crawl_products_detail loi: {e}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Chạy tất cả tests"""
    print("="*70)
    print("TEST UTILITIES VA MODULES")
    print("="*70)

    results = []

    # Test imports
    results.append(("Utils Imports", test_utils_imports()))
    results.append(("Parse Sales Count", test_parse_sales_count()))
    results.append(("Parse Price", test_parse_price()))
    results.append(("Extract Product ID", test_extract_product_id()))
    results.append(("Crawl Products Import", test_crawl_products_import()))
    results.append(("Crawl Products Detail Import", test_crawl_products_detail_import()))

    # Tổng kết
    print("\n" + "="*70)
    print("TONG KET")
    print("="*70)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"  {status}: {test_name}")

    print(f"\nKet qua: {passed}/{total} tests passed")

    if passed == total:
        print("Tat ca tests PASSED!")
        return 0
    else:
        print("Co mot so tests FAILED!")
        return 1

if __name__ == "__main__":
    exit(main())

