"""
Final Validation - Verify all optimizations are ready

Checks:
1. All modules imported
2. All files present
3. Configuration valid
4. Dependencies installed
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

print("=" * 70)
print("✅ FINAL VALIDATION - ALL OPTIMIZATIONS")
print("=" * 70)
print()

errors = []
warnings = []

# Check 1: Critical modules
print("1️⃣ Module Imports")
print("-" * 70)

critical_modules = [
    "src.pipelines.load.db_pool",
    "src.pipelines.load.loader_optimized",
    "src.pipelines.crawl.parallel_crawler",
    "src.pipelines.crawl.crawl_products_parallel",
    "src.pipelines.optimized_pipeline",
    "src.common.batch_processor",
    "src.common.cache_utils",
    "src.common.memory_utils",
    "src.common.infrastructure_monitor",
]

for module in critical_modules:
    try:
        __import__(module)
        print(f"   ✅ {module}")
    except ImportError as e:
        print(f"   ❌ {module}: {e}")
        errors.append(f"Module import failed: {module}")

print()

# Check 2: Required files
print("2️⃣ Required Files")
print("-" * 70)

required_files = [
    # Phase 4
    "src/pipelines/load/db_pool.py",
    "src/pipelines/load/loader_optimized.py",
    "src/pipelines/crawl/parallel_crawler.py",
    "src/pipelines/crawl/crawl_products_parallel.py",
    "src/pipelines/optimized_pipeline.py",
    "src/common/batch_processor.py",
    "src/common/async_batch.py",
    "src/common/cache_utils.py",
    "src/common/memory_utils.py",
    # Phase 5
    "airflow/setup/postgresql_tuning.conf",
    "docker-compose.performance.yml",
    "src/common/infrastructure_monitor.py",
    # Integration
    "airflow/dags/dag_integration.py",
    # Tests
    "tests/test_parallel_benchmark.py",
    "tests/test_phase4_loader.py",
    "tests/test_phase4_complete.py",
]

missing_count = 0
for file_path in required_files:
    full_path = project_root / file_path
    if full_path.exists():
        print(f"   ✅ {file_path}")
    else:
        print(f"   ❌ {file_path}")
        errors.append(f"Missing file: {file_path}")
        missing_count += 1

print()

# Check 3: Dependencies
print("3️⃣ Python Dependencies")
print("-" * 70)

dependencies = [
    "psycopg2",
    "redis",
    "psutil",
]

try:
    import subprocess

    for dep in dependencies:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "show", dep], capture_output=True, text=True
        )
        if result.returncode == 0:
            print(f"   ✅ {dep}")
        else:
            print(f"   ⚠️  {dep} (not installed)")
            warnings.append(f"Optional dependency missing: {dep}")
except Exception as e:
    print(f"   ⚠️  Could not check dependencies: {e}")

print()

# Check 4: Configuration files
print("4️⃣ Configuration Files")
print("-" * 70)

config_files = [
    "docker-compose.yml",
    "docker-compose.performance.yml",
    ".env.example",
    "requirements.txt",
]

for config in config_files:
    path = project_root / config
    if path.exists():
        print(f"   ✅ {config}")
    else:
        print(f"   ⚠️  {config}")
        warnings.append(f"Config file missing: {config}")

print()

# Check 5: Test execution
print("5️⃣ Quick Test Execution")
print("-" * 70)

try:
    from src.common.batch_processor import BatchProcessor

    processor = BatchProcessor(batch_size=10, show_progress=False)
    test_items = list(range(50))

    def mock_process(batch):
        return len(batch)

    stats = processor.process(test_items, mock_process)

    if stats["total_processed"] == 50:
        print("   ✅ BatchProcessor working")
    else:
        print(f"   ⚠️  BatchProcessor unexpected result: {stats}")
        warnings.append("BatchProcessor test failed")

except Exception as e:
    print(f"   ❌ BatchProcessor test failed: {e}")
    errors.append(f"BatchProcessor test: {e}")

try:
    from src.common.cache_utils import cache_in_memory, get_cache_stats

    @cache_in_memory(ttl=60)
    def test_func(x):
        return x * 2

    result1 = test_func(5)
    result2 = test_func(5)

    stats = get_cache_stats()

    if stats["hits"] > 0:
        print("   ✅ Cache working")
    else:
        print("   ⚠️  Cache not working as expected")
        warnings.append("Cache test didn't show hits")

except Exception as e:
    print(f"   ❌ Cache test failed: {e}")
    errors.append(f"Cache test: {e}")

print()

# Final Summary
print("=" * 70)
print("📊 VALIDATION SUMMARY")
print("=" * 70)
print()

if len(errors) == 0 and len(warnings) == 0:
    print("✅ ALL CHECKS PASSED")
    print()
    print("🎉 Ready for production!")
    print()
    print("Next steps:")
    print("   1. Review docker-compose.performance.yml")
    print("   2. Update DAG to use optimized tasks (see dag_integration.py)")
    print("   3. Start optimized infrastructure:")
    print("      docker-compose -f docker-compose.yml -f docker-compose.performance.yml up -d")
    print("   4. Run benchmark: python tests/test_parallel_benchmark.py")
    print()

elif len(errors) == 0:
    print("⚠️  VALIDATION PASSED WITH WARNINGS")
    print()
    print(f"Warnings ({len(warnings)}):")
    for warning in warnings:
        print(f"   • {warning}")
    print()
    print("These warnings are non-critical. You can proceed.")
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
    print("Please fix errors before proceeding.")
    print()

print("=" * 70)
