"""Debug test to find hanging issue."""

import time

print("Testing time.perf_counter()...")
start = time.perf_counter()
time.sleep(0.001)
end = time.perf_counter()
print(f"✅ perf_counter works: {(end - start) * 1000:.2f}ms")

print("\nTesting time.process_time()...")
start_cpu = time.process_time()
x = sum(range(1000))
end_cpu = time.process_time()
print(f"✅ process_time works: {(end_cpu - start_cpu) * 1000:.2f}ms")

print("\nTesting ComponentProfiler...")
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

# Disable psutil
import common.profiling.component_profiler as cp_module
cp_module.PSUTIL_AVAILABLE = False

from common.profiling import ComponentProfiler

profiler = ComponentProfiler()
print("✅ ComponentProfiler created")

print("Starting component...")
profiler.start_component("Test")
print("✅ Component started")

print("Doing work...")
x = sum(range(100))
print("✅ Work done")

print("Sampling memory...")
profiler.sample_memory("Test")
print("✅ Memory sampled")

print("Ending component...")
stats = profiler.end_component("Test", items_processed=10)
print(f"✅ Component ended: {stats.execution_time_ms:.2f}ms")

print("\n✅✅✅ ALL TESTS PASSED!")
