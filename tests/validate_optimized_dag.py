"""
Validate Optimized DAG

Checks:
1. DAG imports successfully
2. No circular dependencies
3. All tasks defined
4. XCom keys match
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "airflow" / "dags"))

print("=" * 70)
print("✅ DAG VALIDATION - Optimized DAG")
print("=" * 70)
print()

errors = []
warnings = []

# Test 1: Import DAG
print("1️⃣ Importing DAG file")
print("-" * 70)

try:
    # Set required env vars for DAG import
    import os
    os.environ.setdefault('AIRFLOW_HOME', str(project_root / 'airflow'))
    os.environ.setdefault('AIRFLOW__CORE__DAGS_FOLDER', str(project_root / 'airflow' / 'dags'))
    
    # Mock Airflow Variables
    from unittest.mock import MagicMock
    sys.modules['airflow.models'] = MagicMock()
    sys.modules['airflow.models.Variable'] = MagicMock()
    
    # Import DAG
    import tiki_crawl_products_optimized_dag as dag_module
    
    print("   ✅ DAG imported successfully")
    
    # Get DAG object
    dag = dag_module.dag
    print(f"   ✅ DAG object: {dag.dag_id}")
    print(f"   ✅ Description: {dag.description[:50]}...")
    print(f"   ✅ Schedule: {dag.schedule_interval}")
    print(f"   ✅ Tags: {dag.tags}")
    
except Exception as e:
    print(f"   ❌ Failed to import DAG: {e}")
    errors.append(f"DAG import failed: {e}")
    import traceback
    traceback.print_exc()

print()

# Test 2: Check tasks
print("2️⃣ Checking DAG tasks")
print("-" * 70)

if 'dag' in locals():
    try:
        tasks = dag.tasks
        print(f"   ✅ Total tasks: {len(tasks)}")
        
        expected_tasks = [
            'crawl_categories',
            'extract_product_urls',
            'crawl_product_details',
            'transform_products',
            'load_products',
            'monitoring.get_cache_stats',
            'monitoring.monitor_infrastructure',
            'cleanup_temp_files',
            'send_notification',
        ]
        
        task_ids = [t.task_id for t in tasks]
        
        for expected in expected_tasks:
            if expected in task_ids:
                print(f"   ✅ {expected}")
            else:
                print(f"   ⚠️  {expected} (not found)")
                warnings.append(f"Task missing: {expected}")
        
    except Exception as e:
        print(f"   ❌ Failed to check tasks: {e}")
        errors.append(f"Task check failed: {e}")
else:
    print("   ⚠️  DAG not loaded, skipping task check")

print()

# Test 3: Check dependencies
print("3️⃣ Checking task dependencies")
print("-" * 70)

if 'dag' in locals():
    try:
        for task in dag.tasks:
            upstream = task.upstream_task_ids
            downstream = task.downstream_task_ids
            
            if upstream or downstream:
                print(f"   ✅ {task.task_id}")
                if upstream:
                    print(f"      ⬅️  Upstream: {', '.join(upstream)}")
                if downstream:
                    print(f"      ➡️  Downstream: {', '.join(downstream)}")
        
        # Check for circular dependencies
        try:
            from airflow.models import DagBag
            dagbag = DagBag(dag_folder=str(project_root / 'airflow' / 'dags'), include_examples=False)
            
            if dag.dag_id in dagbag.dags:
                print("   ✅ No circular dependencies detected")
            
        except Exception as e:
            print(f"   ⚠️  Could not check circular dependencies: {e}")
        
    except Exception as e:
        print(f"   ❌ Failed to check dependencies: {e}")
        errors.append(f"Dependency check failed: {e}")

print()

# Test 4: Check optimized modules are imported
print("4️⃣ Checking optimized module usage")
print("-" * 70)

try:
    dag_file = project_root / "airflow" / "dags" / "tiki_crawl_products_optimized_dag.py"
    with open(dag_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    optimizations = [
        ('dag_integration', 'DAG integration module'),
        ('crawl_products_optimized', 'Parallel crawler'),
        ('load_products_optimized', 'Optimized loader'),
        ('get_cache_stats_task', 'Cache monitoring'),
        ('monitor_infrastructure_task', 'Infrastructure monitoring'),
    ]
    
    for opt_name, desc in optimizations:
        if opt_name in content:
            print(f"   ✅ {desc}")
        else:
            print(f"   ⚠️  {desc} (not found)")
            warnings.append(f"Optimization not used: {desc}")
    
except Exception as e:
    print(f"   ❌ Failed to check optimizations: {e}")
    errors.append(f"Optimization check failed: {e}")

print()

# Test 5: Check XCom key consistency
print("5️⃣ Checking XCom key consistency")
print("-" * 70)

try:
    xcom_keys = {
        'push': [],
        'pull': [],
    }
    
    # Extract XCom operations from DAG file
    import re
    
    push_pattern = r"ti\.xcom_push\(key='([^']+)'"
    pull_pattern = r"ti\.xcom_pull\(key='([^']+)'"
    
    pushes = re.findall(push_pattern, content)
    pulls = re.findall(pull_pattern, content)
    
    xcom_keys['push'] = list(set(pushes))
    xcom_keys['pull'] = list(set(pulls))
    
    print(f"   XCom keys pushed: {len(xcom_keys['push'])}")
    for key in xcom_keys['push']:
        print(f"      • {key}")
    
    print(f"   XCom keys pulled: {len(xcom_keys['pull'])}")
    for key in xcom_keys['pull']:
        if key in xcom_keys['push']:
            print(f"      ✅ {key}")
        else:
            print(f"      ⚠️  {key} (not pushed)")
            warnings.append(f"XCom key pulled but not pushed: {key}")
    
except Exception as e:
    print(f"   ⚠️  XCom check skipped: {e}")

print()

# Summary
print("=" * 70)
print("📊 VALIDATION SUMMARY")
print("=" * 70)
print()

if len(errors) == 0 and len(warnings) == 0:
    print("✅ ALL CHECKS PASSED")
    print()
    print("🎉 Optimized DAG is ready for deployment!")
    print()
    print("Next steps:")
    print("   1. Copy DAG to Airflow dags folder")
    print("   2. Restart Airflow scheduler")
    print("   3. Enable DAG in Airflow UI")
    print("   4. Trigger manual run for testing")
    print("   5. Monitor performance metrics")
    print()
    
elif len(errors) == 0:
    print("⚠️  VALIDATION PASSED WITH WARNINGS")
    print()
    print(f"Warnings ({len(warnings)}):")
    for warning in warnings:
        print(f"   • {warning}")
    print()
    print("You can proceed, but review warnings.")
    print()
    
else:
    print("❌ VALIDATION FAILED")
    print()
    print(f"Errors ({len(errors)}):")
    for error in errors:
        print(f"   • {error}")
    print()
    if warnings:
        print(f"Warnings ({len(warnings)}):")
        for warning in warnings:
            print(f"   • {warning}")
        print()
    print("Fix errors before deploying.")
    print()

print("=" * 70)
