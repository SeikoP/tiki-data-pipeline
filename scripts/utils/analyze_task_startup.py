"""Analyze Airflow task startup time and identify bottlenecks.

This script helps identify why tasks take long to start.
"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))


def measure_import_time(module_name: str) -> float:
    """Measure time to import a module."""
    start = time.time()
    try:
        __import__(module_name)
        elapsed = time.time() - start
        return elapsed
    except Exception as e:
        print(f"   ❌ Failed to import {module_name}: {e}")
        return -1


def analyze_dag_imports():
    """Analyze import times for DAG modules."""
    print("\n" + "=" * 70)
    print("📊 DAG IMPORT TIME ANALYSIS")
    print("=" * 70)

    modules = [
        ("airflow", "Airflow core"),
        ("airflow.operators.python", "PythonOperator"),
        ("psycopg2", "PostgreSQL driver"),
        ("redis", "Redis client"),
        ("selenium", "Selenium WebDriver"),
        ("requests", "HTTP library"),
        ("pandas", "Pandas"),
        ("numpy", "NumPy"),
    ]

    results = []

    for module, description in modules:
        print(f"\n📦 Testing: {description} ({module})")
        elapsed = measure_import_time(module)
        if elapsed >= 0:
            print(f"   ⏱️  Import time: {elapsed:.3f}s")
            results.append((module, description, elapsed))
        else:
            results.append((module, description, -1))

    # Sort by time
    results.sort(key=lambda x: x[2] if x[2] >= 0 else 0, reverse=True)

    print("\n" + "=" * 70)
    print("📈 SLOWEST IMPORTS")
    print("=" * 70)

    for module, description, elapsed in results[:5]:
        if elapsed >= 0:
            print(f"{elapsed:6.3f}s - {description} ({module})")


def analyze_bootstrap_imports():
    """Analyze bootstrap.py import time."""
    print("\n" + "=" * 70)
    print("📊 BOOTSTRAP.PY IMPORT ANALYSIS")
    print("=" * 70)

    print("\n⏱️  Measuring bootstrap import time...")
    start = time.time()

    try:
        # Add DAG path
        dag_path = Path(__file__).parent.parent.parent / "airflow" / "dags"
        sys.path.insert(0, str(dag_path))

        import tiki_crawl_products_v2.bootstrap as bootstrap

        elapsed = time.time() - start
        print(f"   Total time: {elapsed:.3f}s")

        # Check what's in bootstrap
        print("\n📋 Bootstrap contents:")
        attrs = [attr for attr in dir(bootstrap) if not attr.startswith("_")]
        print(f"   - {len(attrs)} exported symbols")

        # Check for heavy objects
        heavy_imports = []
        for attr in attrs:
            obj = getattr(bootstrap, attr)
            if hasattr(obj, "__module__"):
                module = obj.__module__
                if module and not module.startswith("builtins"):
                    heavy_imports.append((attr, module))

        if heavy_imports:
            print("\n   Heavy imports detected:")
            for attr, module in heavy_imports[:10]:
                print(f"      - {attr} from {module}")

    except Exception as e:
        print(f"   ❌ Failed: {e}")
        import traceback

        traceback.print_exc()


def check_airflow_config():
    """Check Airflow configuration for performance issues."""
    print("\n" + "=" * 70)
    print("⚙️  AIRFLOW CONFIGURATION CHECK")
    print("=" * 70)

    import os

    configs = {
        "AIRFLOW__CORE__PARALLELISM": "Max parallel tasks",
        "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG": "Max active tasks per DAG",
        "AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC": "Scheduler heartbeat",
        "AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL": "Min file process interval",
        "AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT": "DAG import timeout",
        "AIRFLOW__CORE__TASK_RUNNER": "Task runner",
        "AIRFLOW__CORE__EXECUTOR": "Executor type",
    }

    print("\n📋 Current settings:")
    for key, description in configs.items():
        value = os.environ.get(key, "Not set")
        print(f"   {description:30s}: {value}")


def measure_task_overhead():
    """Measure overhead of task execution."""
    print("\n" + "=" * 70)
    print("⏱️  TASK EXECUTION OVERHEAD")
    print("=" * 70)

    print("\n1. Measuring Python startup time...")
    start = time.time()
    import subprocess

    subprocess.run(
        [sys.executable, "-c", "print('hello')"], capture_output=True, text=True
    )
    python_startup = time.time() - start
    print(f"   Python startup: {python_startup:.3f}s")

    print("\n2. Measuring import overhead...")
    start = time.time()

    elapsed = time.time() - start
    print(f"   Basic imports: {elapsed:.3f}s")

    print("\n3. Measuring sys.path overhead...")
    start = time.time()
    sys.path.insert(0, "/opt/airflow/src")
    elapsed = time.time() - start
    print(f"   sys.path modification: {elapsed:.6f}s")


def recommendations():
    """Provide recommendations for improving task startup time."""
    print("\n" + "=" * 70)
    print("💡 RECOMMENDATIONS")
    print("=" * 70)

    print("""
1. ⚡ Use lazy imports in tasks
   - Import heavy modules inside task functions, not at module level
   - Example: import selenium only when needed

2. 🔄 Optimize PYTHONPATH
   - Reduce number of paths in PYTHONPATH
   - Use absolute imports instead of relative

3. 📦 Reduce DAG file size
   - Move task logic to separate modules
   - Use dynamic imports for task-specific code

4. ⚙️  Tune Airflow settings
   - Increase AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL (reduce DAG parsing)
   - Use StandardTaskRunner (faster than BashTaskRunner)
   - Reduce AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC

5. 🐳 Docker optimization
   - Use smaller base images
   - Pre-compile Python files (.pyc)
   - Use volume mounts efficiently

6. 💾 Database optimization
   - Ensure PostgreSQL has enough connections
   - Tune shared_buffers and work_mem
   - Add indexes on frequently queried columns

7. 🚀 Task-level optimization
   - Use task pools to limit concurrency
   - Set appropriate task timeouts
   - Use XCom efficiently (avoid large data)

8. 📊 Monitor and profile
   - Use Airflow metrics
   - Profile task execution with cProfile
   - Monitor container resource usage
    """)


def main():
    """Run all analyses."""
    print("\n" + "=" * 70)
    print("🔍 AIRFLOW TASK STARTUP ANALYSIS")
    print("=" * 70)

    # Analysis 1: Import times
    analyze_dag_imports()

    # Analysis 2: Bootstrap import
    analyze_bootstrap_imports()

    # Analysis 3: Airflow config
    check_airflow_config()

    # Analysis 4: Task overhead
    measure_task_overhead()

    # Recommendations
    recommendations()

    print("\n" + "=" * 70)
    print("✅ Analysis complete")
    print("=" * 70)


if __name__ == "__main__":
    main()
