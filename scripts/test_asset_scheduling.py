#!/usr/bin/env python3
"""
Script để test Asset Scheduling

Usage:
    python scripts/test_asset_scheduling.py
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from airflow.models import Variable, DagBag
    from airflow.datasets import Dataset
except ImportError as e:
    print(f"❌ Không thể import Airflow: {e}")
    print("   Hãy chạy script này trong môi trường Airflow")
    sys.exit(1)


def test_asset_definitions():
    """Test asset definitions trong dag_assets"""
    print("=" * 70)
    print("🧪 Test Asset Definitions")
    print("=" * 70)
    
    try:
        # Import asset definitions
        sys.path.insert(0, str(project_root / "airflow" / "dags"))
        from dag_assets import (
            RAW_PRODUCTS_DATASET,
            PRODUCTS_WITH_DETAIL_DATASET,
            TRANSFORMED_PRODUCTS_DATASET,
            FINAL_PRODUCTS_DATASET,
            get_outlets_for_task,
        )
        
        # Test datasets
        datasets = [
            ("RAW_PRODUCTS_DATASET", RAW_PRODUCTS_DATASET, "tiki://products/raw"),
            ("PRODUCTS_WITH_DETAIL_DATASET", PRODUCTS_WITH_DETAIL_DATASET, "tiki://products/with_detail"),
            ("TRANSFORMED_PRODUCTS_DATASET", TRANSFORMED_PRODUCTS_DATASET, "tiki://products/transformed"),
            ("FINAL_PRODUCTS_DATASET", FINAL_PRODUCTS_DATASET, "tiki://products/final"),
        ]
        
        all_passed = True
        for name, dataset, expected_uri in datasets:
            if dataset is None:
                print(f"⚠️  {name}: None (Asset scheduling có thể tắt)")
            elif hasattr(dataset, 'uri'):
                if dataset.uri == expected_uri:
                    print(f"✅ {name}: {dataset.uri}")
                else:
                    print(f"❌ {name}: Expected {expected_uri}, got {dataset.uri}")
                    all_passed = False
            else:
                print(f"❌ {name}: Không có attribute 'uri'")
                all_passed = False
        
        # Test get_outlets_for_task
        print("\n🧪 Test get_outlets_for_task():")
        test_tasks = ["save_products", "save_products_with_detail", "transform_products", "load_products"]
        for task in test_tasks:
            outlets = get_outlets_for_task(task)
            if outlets is None:
                print(f"⚠️  {task}: None (Asset scheduling có thể tắt)")
            else:
                print(f"✅ {task}: {[d.uri if hasattr(d, 'uri') else str(d) for d in outlets]}")
        
        return all_passed
        
    except Exception as e:
        print(f"❌ Lỗi khi test asset definitions: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_dag_parsing():
    """Test DAG parsing với Asset"""
    print("\n" + "=" * 70)
    print("🧪 Test DAG Parsing")
    print("=" * 70)
    
    try:
        dag_folder = project_root / "airflow" / "dags"
        dag_bag = DagBag(dag_folder=str(dag_folder), include_examples=False)
        
        if dag_bag.import_errors:
            print("❌ Có lỗi khi parse DAGs:")
            for dag_id, error in dag_bag.import_errors.items():
                print(f"   {dag_id}: {error}")
            return False
        
        print(f"✅ Đã parse {len(dag_bag.dags)} DAGs")
        
        # Test tiki_crawl_products DAG
        dag_id = "tiki_crawl_products"
        if dag_id in dag_bag.dags:
            dag = dag_bag.dags[dag_id]
            print(f"✅ Tìm thấy DAG: {dag_id}")
            
            # Check tasks có outlets không
            tasks_with_outlets = []
            for task_id, task in dag.task_dict.items():
                if hasattr(task, 'outlets') and task.outlets:
                    tasks_with_outlets.append(task_id)
            
            if tasks_with_outlets:
                print(f"✅ Tìm thấy {len(tasks_with_outlets)} tasks có outlets:")
                for task_id in tasks_with_outlets:
                    task = dag.task_dict[task_id]
                    outlets = [str(o) for o in task.outlets]
                    print(f"   - {task_id}: {outlets}")
            else:
                print("⚠️  Không có tasks nào có outlets (Asset scheduling có thể tắt)")
            
            return True
        else:
            print(f"❌ Không tìm thấy DAG: {dag_id}")
            print(f"   Available DAGs: {list(dag_bag.dags.keys())}")
            return False
            
    except Exception as e:
        print(f"❌ Lỗi khi test DAG parsing: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_variable():
    """Test Variable"""
    print("\n" + "=" * 70)
    print("🧪 Test Variable")
    print("=" * 70)
    
    try:
        value = Variable.get("TIKI_USE_ASSET_SCHEDULING", default_var="false")
        is_enabled = value.lower() == "true"
        
        print(f"Variable TIKI_USE_ASSET_SCHEDULING: {value}")
        if is_enabled:
            print("✅ Asset Scheduling: ENABLED")
        else:
            print("❌ Asset Scheduling: DISABLED")
            print("   Chạy: python scripts/setup_asset_scheduling.py --enable")
        
        return True
    except Exception as e:
        print(f"❌ Lỗi khi test Variable: {e}")
        return False


def main():
    print("🧪 Testing Asset Scheduling Setup")
    print("=" * 70)
    
    results = []
    
    # Test 1: Variable
    results.append(("Variable", test_variable()))
    
    # Test 2: Asset Definitions
    results.append(("Asset Definitions", test_asset_definitions()))
    
    # Test 3: DAG Parsing
    results.append(("DAG Parsing", test_dag_parsing()))
    
    # Summary
    print("\n" + "=" * 70)
    print("📊 Test Summary")
    print("=" * 70)
    
    all_passed = True
    for test_name, passed in results:
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name}: {status}")
        if not passed:
            all_passed = False
    
    print("=" * 70)
    if all_passed:
        print("✅ Tất cả tests PASSED!")
        sys.exit(0)
    else:
        print("❌ Một số tests FAILED. Vui lòng kiểm tra lại.")
        sys.exit(1)


if __name__ == "__main__":
    main()

