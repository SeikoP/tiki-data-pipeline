"""
Script tổng hợp để kiểm tra lỗi trong dự án
"""

import json
import os
import sys


def check_imports():
    """Kiểm tra imports có hoạt động không"""
    print("=" * 70)
    print("KIEM TRA IMPORTS")
    print("=" * 70)

    errors = []

    # Test utils import
    try:
        sys.path.insert(
            0, os.path.join(os.path.dirname(__file__), "..", "src", "pipelines", "crawl")
        )
        print("OK: Utils import thanh cong")
    except Exception as e:
        errors.append(f"Utils import loi: {e}")
        print(f"FAIL: Utils import loi: {e}")

    # Test crawl_products import
    try:
        import importlib.util

        crawl_path = os.path.join(
            os.path.dirname(__file__), "..", "src", "pipelines", "crawl", "crawl_products.py"
        )
        if os.path.exists(crawl_path):
            spec = importlib.util.spec_from_file_location("test_crawl", crawl_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                print("OK: crawl_products import thanh cong")
            else:
                errors.append("Khong the load crawl_products spec")
        else:
            errors.append("Khong tim thay crawl_products.py")
    except Exception as e:
        errors.append(f"crawl_products import loi: {e}")
        print(f"FAIL: crawl_products import loi: {e}")

    # Test crawl_products_detail import
    try:
        detail_path = os.path.join(
            os.path.dirname(__file__), "..", "src", "pipelines", "crawl", "crawl_products_detail.py"
        )
        if os.path.exists(detail_path):
            spec = importlib.util.spec_from_file_location("test_detail", detail_path)
            if spec and spec.loader:
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                print("OK: crawl_products_detail import thanh cong")
            else:
                errors.append("Khong the load crawl_products_detail spec")
        else:
            errors.append("Khong tim thay crawl_products_detail.py")
    except Exception as e:
        errors.append(f"crawl_products_detail import loi: {e}")
        print(f"FAIL: crawl_products_detail import loi: {e}")

    return errors


def check_file_structure():
    """Kiểm tra cấu trúc file"""
    print("\n" + "=" * 70)
    print("KIEM TRA CAU TRUC FILE")
    print("=" * 70)

    errors = []
    required_files = [
        "src/pipelines/crawl/utils.py",
        "src/pipelines/crawl/crawl_products.py",
        "src/pipelines/crawl/crawl_products_detail.py",
        "airflow/dags/tiki_crawl_products_dag.py",
    ]

    for file_path in required_files:
        full_path = os.path.join(os.path.dirname(__file__), "..", file_path)
        if os.path.exists(full_path):
            print(f"OK: {file_path}")
        else:
            errors.append(f"Khong tim thay: {file_path}")
            print(f"FAIL: Khong tim thay {file_path}")

    return errors


def check_syntax():
    """Kiểm tra syntax errors"""
    print("\n" + "=" * 70)
    print("KIEM TRA SYNTAX")
    print("=" * 70)

    errors = []
    python_files = [
        "src/pipelines/crawl/utils.py",
        "src/pipelines/crawl/crawl_products.py",
        "src/pipelines/crawl/crawl_products_detail.py",
    ]

    for file_path in python_files:
        full_path = os.path.join(os.path.dirname(__file__), "..", file_path)
        if os.path.exists(full_path):
            try:
                with open(full_path, encoding="utf-8") as f:
                    compile(f.read(), full_path, "exec")
                print(f"OK: {file_path} - syntax hop le")
            except SyntaxError as e:
                errors.append(f"{file_path}: Syntax error at line {e.lineno}: {e.msg}")
                print(f"FAIL: {file_path} - syntax error: {e}")
            except Exception as e:
                errors.append(f"{file_path}: {e}")
                print(f"FAIL: {file_path} - {e}")

    return errors


def check_data_files():
    """Kiểm tra data files"""
    print("\n" + "=" * 70)
    print("KIEM TRA DATA FILES")
    print("=" * 70)

    errors = []
    data_files = [
        "data/raw/products/products.json",
        "data/raw/products/products_with_detail.json",
    ]

    for file_path in data_files:
        full_path = os.path.join(os.path.dirname(__file__), "..", file_path)
        if os.path.exists(full_path):
            try:
                with open(full_path, encoding="utf-8") as f:
                    data = json.load(f)
                products = data.get("products", [])
                print(f"OK: {file_path} - {len(products)} products")
            except Exception as e:
                errors.append(f"{file_path}: {e}")
                print(f"FAIL: {file_path} - {e}")
        else:
            print(f"INFO: {file_path} khong ton tai (co the chua chay DAG)")

    return errors


def main():
    """Chạy tất cả checks"""
    print("=" * 70)
    print("KIEM TRA LOI TRONG DU AN")
    print("=" * 70)

    all_errors = []

    all_errors.extend(check_imports())
    all_errors.extend(check_file_structure())
    all_errors.extend(check_syntax())
    all_errors.extend(check_data_files())

    # Tổng kết
    print("\n" + "=" * 70)
    print("TONG KET")
    print("=" * 70)

    if all_errors:
        print(f"Tim thay {len(all_errors)} loi:")
        for i, error in enumerate(all_errors, 1):
            print(f"  {i}. {error}")
        return 1
    else:
        print("Khong tim thay loi nao!")
        print("Tat ca checks PASSED!")
        return 0


if __name__ == "__main__":
    exit(main())
