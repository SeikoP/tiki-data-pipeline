#!/usr/bin/env python3
"""
Script để setup Asset Scheduling cho Tiki Data Pipeline

Usage:
    python scripts/setup_asset_scheduling.py --enable
    python scripts/setup_asset_scheduling.py --disable
    python scripts/setup_asset_scheduling.py --check
"""

import argparse
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from airflow.models import Variable
    from airflow import __version__ as airflow_version
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    print("WARNING: Airflow không được cài đặt hoặc không có trong PYTHONPATH")
    print("   Script này cần chạy trong môi trường Airflow")
    print("   Hoặc sử dụng Airflow CLI trực tiếp:")
    print("   - airflow variables set TIKI_USE_ASSET_SCHEDULING true")
    print("   - airflow variables get TIKI_USE_ASSET_SCHEDULING")


def check_airflow_version():
    """Kiểm tra Airflow version có hỗ trợ Dataset không"""
    print("=" * 70)
    print("Kiem tra Airflow Version")
    print("=" * 70)
    
    if not AIRFLOW_AVAILABLE:
        print("WARNING: Không thể kiểm tra Airflow version")
        print("   Hãy chạy trong môi trường Airflow hoặc dùng CLI:")
        print("   airflow version")
        return None
    
    version_parts = airflow_version.split(".")
    major = int(version_parts[0])
    minor = int(version_parts[1]) if len(version_parts) > 1 else 0
    
    print(f"Airflow Version: {airflow_version}")
    
    if major >= 2 and minor >= 7:
        print("OK: Airflow version ho tro Dataset (>= 2.7.0)")
        
        # Kiểm tra Dataset có sẵn không
        try:
            from airflow.datasets import Dataset
            print("OK: Dataset class co san")
            return True
        except ImportError:
            print("WARNING: Dataset class khong co san (co the can cai dat them)")
            return False
    else:
        print(f"ERROR: Airflow version {airflow_version} khong ho tro Dataset")
        print("   Yeu cau: Airflow >= 2.7.0")
        return False


def check_asset_scheduling():
    """Kiểm tra trạng thái Asset Scheduling"""
    print("=" * 70)
    print("Kiem tra Asset Scheduling Status")
    print("=" * 70)
    
    if not AIRFLOW_AVAILABLE:
        print("WARNING: Khong the doc Variable (Airflow khong co san)")
        print("   Hay dung CLI: airflow variables get TIKI_USE_ASSET_SCHEDULING")
        return None
    
    try:
        value = Variable.get("TIKI_USE_ASSET_SCHEDULING", default_var="false")
        is_enabled = value.lower() == "true"
        
        if is_enabled:
            print("OK: Asset Scheduling: ENABLED")
        else:
            print("INFO: Asset Scheduling: DISABLED")
        
        print(f"   Variable value: {value}")
        return is_enabled
    except Exception as e:
        print(f"WARNING: Khong the doc Variable: {e}")
        print("   Mac dinh: DISABLED")
        return False


def enable_asset_scheduling():
    """Bật Asset Scheduling"""
    print("=" * 70)
    print("Bat Asset Scheduling")
    print("=" * 70)
    
    if not AIRFLOW_AVAILABLE:
        print("ERROR: Khong the set Variable (Airflow khong co san)")
        print("   Hay dung CLI:")
        print("   airflow variables set TIKI_USE_ASSET_SCHEDULING true")
        return False
    
    # Kiểm tra version trước
    version_check = check_airflow_version()
    if version_check is False:
        print("\nERROR: Khong the bat Asset Scheduling: Airflow version khong ho tro")
        return False
    
    try:
        Variable.set("TIKI_USE_ASSET_SCHEDULING", "true")
        print("OK: Da set Variable: TIKI_USE_ASSET_SCHEDULING = true")
        
        # Verify
        value = Variable.get("TIKI_USE_ASSET_SCHEDULING")
        if value.lower() == "true":
            print("OK: Verified: Asset Scheduling da duoc bat")
            return True
        else:
            print("ERROR: Variable khong duoc set dung")
            return False
    except Exception as e:
        print(f"ERROR: Loi khi set Variable: {e}")
        return False


def disable_asset_scheduling():
    """Tắt Asset Scheduling"""
    print("=" * 70)
    print("Tat Asset Scheduling")
    print("=" * 70)
    
    if not AIRFLOW_AVAILABLE:
        print("ERROR: Khong the set Variable (Airflow khong co san)")
        print("   Hay dung CLI:")
        print("   airflow variables set TIKI_USE_ASSET_SCHEDULING false")
        return False
    
    try:
        Variable.set("TIKI_USE_ASSET_SCHEDULING", "false")
        print("OK: Da set Variable: TIKI_USE_ASSET_SCHEDULING = false")
        
        # Verify
        value = Variable.get("TIKI_USE_ASSET_SCHEDULING")
        if value.lower() == "false":
            print("OK: Verified: Asset Scheduling da duoc tat")
            return True
        else:
            print("ERROR: Variable khong duoc set dung")
            return False
    except Exception as e:
        print(f"ERROR: Loi khi set Variable: {e}")
        return False


def list_datasets():
    """Liệt kê các datasets được định nghĩa"""
    print("=" * 70)
    print("📊 Datasets được định nghĩa")
    print("=" * 70)
    
    datasets = [
        ("tiki://products/raw", "Raw products từ crawl", "save_products"),
        ("tiki://products/with_detail", "Products với chi tiết", "save_products_with_detail"),
        ("tiki://products/transformed", "Products đã transform", "transform_products"),
        ("tiki://products/final", "Products đã load vào DB", "load_products"),
    ]
    
    print(f"{'Dataset URI':<35} {'Mô tả':<40} {'Tạo bởi Task':<25}")
    print("-" * 100)
    for uri, desc, task in datasets:
        print(f"{uri:<35} {desc:<40} {task:<25}")
    
    print("\n💡 Các datasets này sẽ được tạo khi Asset Scheduling được bật")


def main():
    parser = argparse.ArgumentParser(
        description="Setup Asset Scheduling cho Tiki Data Pipeline"
    )
    parser.add_argument(
        "--enable",
        action="store_true",
        help="Bật Asset Scheduling"
    )
    parser.add_argument(
        "--disable",
        action="store_true",
        help="Tắt Asset Scheduling"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Kiểm tra trạng thái Asset Scheduling"
    )
    parser.add_argument(
        "--list-datasets",
        action="store_true",
        help="Liệt kê các datasets được định nghĩa"
    )
    
    args = parser.parse_args()
    
    if args.enable:
        if enable_asset_scheduling():
            print("\n" + "=" * 70)
            print("OK: Setup thanh cong!")
            print("=" * 70)
            print("\nNext steps:")
            print("1. Restart Airflow scheduler (neu can)")
            print("2. Kiem tra DAG trong Airflow UI")
            print("3. Vao menu 'Datasets' de xem datasets")
            print("4. Trigger DAG va verify datasets duoc tao")
            sys.exit(0)
        else:
            sys.exit(1)
    
    elif args.disable:
        if disable_asset_scheduling():
            print("\n" + "=" * 70)
            print("✅ Asset Scheduling đã được tắt")
            print("=" * 70)
            sys.exit(0)
        else:
            sys.exit(1)
    
    elif args.check:
        check_airflow_version()
        print()
        check_asset_scheduling()
        print()
        list_datasets()
        sys.exit(0)
    
    elif args.list_datasets:
        list_datasets()
        sys.exit(0)
    
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()

