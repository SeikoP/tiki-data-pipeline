"""Simple test to debug hanging issue."""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from common.profiling import FunctionProfiler

def test_simple_profiler():
    """Test basic profiler functionality."""
    profiler = FunctionProfiler()
    
    @profiler.profile
    def test_func():
        x = sum(range(10))
        return x
    
    # Call once
    result = test_func()
    assert result == 45
    
    # Get stats
    stats = profiler.get_stats()
    assert len(stats) > 0
    
    print("✅ Test passed!")

if __name__ == "__main__":
    test_simple_profiler()
