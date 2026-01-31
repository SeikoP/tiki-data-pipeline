"""Property-based tests for configuration optimizer.

These tests verify correctness properties of the ConfigurationAnalyzer
using property-based testing with hypothesis.

Feature: performance-optimization
Property 5: Configuration Recommendation Validity
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

try:
    from hypothesis import assume, given, settings
    from hypothesis import strategies as st

    HYPOTHESIS_AVAILABLE = True
except ImportError:
    HYPOTHESIS_AVAILABLE = False
    print("⚠️ Hypothesis not installed. Install with: pip install hypothesis")

from common.configuration_analyzer import ConfigurationAnalyzer, OptimizedConfig, SystemCapabilities


# Helper strategies for generating test data
def system_capabilities_strategy():
    """Strategy for generating SystemCapabilities."""
    return st.builds(
        SystemCapabilities,
        cpu_cores_physical=st.integers(min_value=1, max_value=64),
        cpu_cores_logical=st.integers(min_value=1, max_value=128),
        cpu_freq_mhz=st.floats(
            min_value=800.0, max_value=5000.0, allow_nan=False, allow_infinity=False
        ),
        total_memory_mb=st.floats(
            min_value=512.0, max_value=131072.0, allow_nan=False, allow_infinity=False
        ),
        available_memory_mb=st.floats(
            min_value=256.0, max_value=65536.0, allow_nan=False, allow_infinity=False
        ),
        disk_read_speed_mbps=st.floats(
            min_value=10.0, max_value=5000.0, allow_nan=False, allow_infinity=False
        ),
        disk_write_speed_mbps=st.floats(
            min_value=10.0, max_value=5000.0, allow_nan=False, allow_infinity=False
        ),
        network_bandwidth_mbps=st.floats(
            min_value=1.0, max_value=1000.0, allow_nan=False, allow_infinity=False
        ),
        platform=st.just("TestPlatform"),
        python_version=st.just("3.11.0"),
    )


# Property 5: Configuration Recommendation Validity
# For any system capabilities and profiling data, all generated configuration
# recommendations should be within valid ranges and include non-empty rationale


def test_property_5_manual_basic():
    """Property 5: Configuration Recommendation Validity - Basic Manual Test

    **Validates: Requirements 3.5, 5.2, 5.3, 7.1, 7.7, 9.1-9.7**

    For any system capabilities, generated configuration should be valid.
    """
    analyzer = ConfigurationAnalyzer()

    # Test with typical system
    system = SystemCapabilities(
        cpu_cores_physical=4,
        cpu_cores_logical=8,
        cpu_freq_mhz=2400.0,
        total_memory_mb=8192.0,
        available_memory_mb=4096.0,
        disk_read_speed_mbps=200.0,
        disk_write_speed_mbps=150.0,
        network_bandwidth_mbps=50.0,
        platform="Linux",
        python_version="3.11.0",
    )

    config = analyzer.recommend_config(system)

    # Verify all fields are present
    assert hasattr(config, "product_batch_size")
    assert hasattr(config, "product_pool_size")
    assert hasattr(config, "http_connector_limit")
    assert hasattr(config, "http_timeout_total")
    assert hasattr(config, "http_timeout_connect")
    assert hasattr(config, "thread_pool_size")
    assert hasattr(config, "process_pool_size")
    assert hasattr(config, "use_vectorization")
    assert hasattr(config, "db_connection_pool_min")
    assert hasattr(config, "db_connection_pool_max")
    assert hasattr(config, "db_batch_size")
    assert hasattr(config, "use_copy_command")
    assert hasattr(config, "redis_cache_ttl")
    assert hasattr(config, "memory_cache_size_mb")
    assert hasattr(config, "cache_eviction_threshold")
    assert hasattr(config, "rationale")

    # Verify rationale is non-empty for all config values
    expected_rationale_keys = [
        "product_batch_size",
        "product_pool_size",
        "http_connector_limit",
        "http_timeout",
        "thread_pool_size",
        "process_pool_size",
        "use_vectorization",
        "db_connection_pool",
        "db_batch_size",
        "use_copy_command",
        "redis_cache_ttl",
        "memory_cache_size_mb",
        "cache_eviction_threshold",
    ]

    for key in expected_rationale_keys:
        assert key in config.rationale, f"Missing rationale for {key}"
        assert len(config.rationale[key]) > 0, f"Empty rationale for {key}"

    # Verify values are within valid ranges
    assert 10 <= config.product_batch_size <= 50000
    assert 1 <= config.product_pool_size <= 100
    assert 10 <= config.http_connector_limit <= 1000
    assert 1 <= config.http_timeout_total <= 300
    assert 1 <= config.http_timeout_connect <= 60
    assert config.http_timeout_connect <= config.http_timeout_total
    assert 1 <= config.thread_pool_size <= 64
    assert 1 <= config.process_pool_size <= 32
    assert isinstance(config.use_vectorization, bool)
    assert 1 <= config.db_connection_pool_min <= 50
    assert config.db_connection_pool_max >= config.db_connection_pool_min
    assert config.db_connection_pool_max <= 100
    assert 100 <= config.db_batch_size <= 10000
    assert isinstance(config.use_copy_command, bool)
    assert 60 <= config.redis_cache_ttl <= 86400
    assert 10 <= config.memory_cache_size_mb <= 2000
    assert 0.5 <= config.cache_eviction_threshold <= 1.0

    print("✅ Property 5: Basic manual test PASSED")


def test_property_5_manual_edge_cases():
    """Property 5: Edge Cases - Low and High Resource Systems

    **Validates: Requirements 9.1-9.7**

    Configuration should be valid for both low and high resource systems.
    """
    analyzer = ConfigurationAnalyzer()

    # Test with low-resource system
    low_system = SystemCapabilities(
        cpu_cores_physical=1,
        cpu_cores_logical=2,
        cpu_freq_mhz=1000.0,
        total_memory_mb=1024.0,
        available_memory_mb=512.0,
        disk_read_speed_mbps=50.0,
        disk_write_speed_mbps=30.0,
        network_bandwidth_mbps=5.0,
        platform="Linux",
        python_version="3.11.0",
    )

    low_config = analyzer.recommend_config(low_system)
    is_valid, warnings = analyzer.validate_config(low_config, low_system)

    assert is_valid, f"Low-resource config should be valid. Warnings: {warnings}"
    assert len(low_config.rationale) > 0, "Should have rationale"

    # Test with high-resource system
    high_system = SystemCapabilities(
        cpu_cores_physical=32,
        cpu_cores_logical=64,
        cpu_freq_mhz=4000.0,
        total_memory_mb=65536.0,
        available_memory_mb=32768.0,
        disk_read_speed_mbps=2000.0,
        disk_write_speed_mbps=1500.0,
        network_bandwidth_mbps=500.0,
        platform="Linux",
        python_version="3.11.0",
    )

    high_config = analyzer.recommend_config(high_system)
    is_valid, warnings = analyzer.validate_config(high_config, high_system)

    assert is_valid, f"High-resource config should be valid. Warnings: {warnings}"
    assert len(high_config.rationale) > 0, "Should have rationale"

    # High-resource system should have larger values
    assert high_config.product_batch_size >= low_config.product_batch_size
    assert high_config.product_pool_size >= low_config.product_pool_size
    assert high_config.process_pool_size >= low_config.process_pool_size

    print("✅ Property 5: Edge cases test PASSED")


def test_property_5_validation_catches_invalid():
    """Property 5: Validation Catches Invalid Configurations

    **Validates: Requirements 9.1-9.4**

    Validation should catch configurations that are outside valid ranges.
    """
    analyzer = ConfigurationAnalyzer()

    system = SystemCapabilities(
        cpu_cores_physical=4,
        cpu_cores_logical=8,
        cpu_freq_mhz=2400.0,
        total_memory_mb=8192.0,
        available_memory_mb=4096.0,
        disk_read_speed_mbps=200.0,
        disk_write_speed_mbps=150.0,
        network_bandwidth_mbps=50.0,
        platform="Linux",
        python_version="3.11.0",
    )

    # Create invalid config with out-of-range values
    invalid_config = OptimizedConfig(
        product_batch_size=100000,  # Too large
        product_pool_size=200,  # Too large
        http_connector_limit=5,  # Too small
        http_timeout_total=500,  # Too large
        http_timeout_connect=100,  # Too large and > total
        thread_pool_size=100,  # Too large
        process_pool_size=50,  # Too large
        use_vectorization=True,
        db_connection_pool_min=100,  # Too large
        db_connection_pool_max=50,  # Less than min
        db_batch_size=50,  # Too small
        use_copy_command=True,
        redis_cache_ttl=30,  # Too small
        memory_cache_size_mb=5000,  # Too large
        cache_eviction_threshold=1.5,  # Too large
        rationale={},
    )

    is_valid, warnings = analyzer.validate_config(invalid_config, system)

    assert not is_valid, "Invalid config should be detected"
    assert len(warnings) > 0, "Should have warnings for invalid values"

    print(f"✅ Property 5: Validation caught {len(warnings)} invalid values")


def test_property_5_auto_detection():
    """Property 5: Auto-Detection Works

    **Validates: Requirements 9.1**

    System capability detection should work without errors.
    """
    analyzer = ConfigurationAnalyzer()

    # Test auto-detection
    system = analyzer.analyze_system()

    # Verify all fields are present and reasonable
    assert system.cpu_cores_physical >= 1
    assert system.cpu_cores_logical >= system.cpu_cores_physical
    assert system.cpu_freq_mhz > 0
    assert system.total_memory_mb > 0
    assert system.available_memory_mb > 0
    assert system.available_memory_mb <= system.total_memory_mb
    assert system.disk_read_speed_mbps > 0
    assert system.disk_write_speed_mbps > 0
    assert system.network_bandwidth_mbps > 0
    assert len(system.platform) > 0
    assert len(system.python_version) > 0

    # Test that config generation works with auto-detected system
    config = analyzer.recommend_config(system)
    is_valid, warnings = analyzer.validate_config(config, system)

    assert is_valid, f"Auto-detected config should be valid. Warnings: {warnings}"

    print("✅ Property 5: Auto-detection test PASSED")


# Hypothesis-based property tests (if available)
if HYPOTHESIS_AVAILABLE:

    @given(system=system_capabilities_strategy())
    @settings(max_examples=100, deadline=None)
    def test_property_5_hypothesis(system):
        """Property 5: Configuration Recommendation Validity (Hypothesis)

        **Validates: Requirements 3.5, 5.2, 5.3, 7.1, 7.7, 9.1-9.7**

        For any system capabilities, all generated configuration recommendations
        should be within valid ranges and include non-empty rationale.
        """
        # Ensure logical cores >= physical cores
        assume(system.cpu_cores_logical >= system.cpu_cores_physical)
        # Ensure available memory <= total memory
        assume(system.available_memory_mb <= system.total_memory_mb)
        # Ensure reasonable memory values
        assume(system.available_memory_mb >= 256.0)

        analyzer = ConfigurationAnalyzer()
        config = analyzer.recommend_config(system)

        # Verify all required fields are present
        assert hasattr(config, "product_batch_size")
        assert hasattr(config, "product_pool_size")
        assert hasattr(config, "http_connector_limit")
        assert hasattr(config, "http_timeout_total")
        assert hasattr(config, "http_timeout_connect")
        assert hasattr(config, "thread_pool_size")
        assert hasattr(config, "process_pool_size")
        assert hasattr(config, "use_vectorization")
        assert hasattr(config, "db_connection_pool_min")
        assert hasattr(config, "db_connection_pool_max")
        assert hasattr(config, "db_batch_size")
        assert hasattr(config, "use_copy_command")
        assert hasattr(config, "redis_cache_ttl")
        assert hasattr(config, "memory_cache_size_mb")
        assert hasattr(config, "cache_eviction_threshold")
        assert hasattr(config, "rationale")

        # Verify rationale is non-empty
        assert len(config.rationale) > 0, "Rationale should not be empty"
        for key, value in config.rationale.items():
            assert isinstance(value, str), f"Rationale for {key} should be string"
            assert len(value) > 0, f"Rationale for {key} should not be empty"

        # Verify values are within valid ranges
        assert 10 <= config.product_batch_size <= 50000, (
            f"product_batch_size {config.product_batch_size} out of range"
        )
        assert 1 <= config.product_pool_size <= 100, (
            f"product_pool_size {config.product_pool_size} out of range"
        )
        assert 10 <= config.http_connector_limit <= 1000, (
            f"http_connector_limit {config.http_connector_limit} out of range"
        )
        assert 1 <= config.http_timeout_total <= 300, (
            f"http_timeout_total {config.http_timeout_total} out of range"
        )
        assert 1 <= config.http_timeout_connect <= 60, (
            f"http_timeout_connect {config.http_timeout_connect} out of range"
        )
        assert config.http_timeout_connect <= config.http_timeout_total, (
            "http_timeout_connect should not exceed http_timeout_total"
        )
        assert 1 <= config.thread_pool_size <= 64, (
            f"thread_pool_size {config.thread_pool_size} out of range"
        )
        assert 1 <= config.process_pool_size <= 32, (
            f"process_pool_size {config.process_pool_size} out of range"
        )
        assert isinstance(config.use_vectorization, bool), "use_vectorization should be boolean"
        assert 1 <= config.db_connection_pool_min <= 50, (
            f"db_connection_pool_min {config.db_connection_pool_min} out of range"
        )
        assert config.db_connection_pool_max >= config.db_connection_pool_min, (
            "db_connection_pool_max should be >= min"
        )
        assert config.db_connection_pool_max <= 100, (
            f"db_connection_pool_max {config.db_connection_pool_max} out of range"
        )
        assert 100 <= config.db_batch_size <= 10000, (
            f"db_batch_size {config.db_batch_size} out of range"
        )
        assert isinstance(config.use_copy_command, bool), "use_copy_command should be boolean"
        assert 60 <= config.redis_cache_ttl <= 86400, (
            f"redis_cache_ttl {config.redis_cache_ttl} out of range"
        )
        assert 10 <= config.memory_cache_size_mb <= 2000, (
            f"memory_cache_size_mb {config.memory_cache_size_mb} out of range"
        )
        assert 0.5 <= config.cache_eviction_threshold <= 1.0, (
            f"cache_eviction_threshold {config.cache_eviction_threshold} out of range"
        )

        # Verify validation passes for generated config
        is_valid, warnings = analyzer.validate_config(config, system)
        assert is_valid, f"Generated config should be valid. Warnings: {warnings}"


if __name__ == "__main__":
    print("Running Property 5: Configuration Recommendation Validity tests...\n")

    test_property_5_manual_basic()
    test_property_5_manual_edge_cases()
    test_property_5_validation_catches_invalid()
    test_property_5_auto_detection()

    if HYPOTHESIS_AVAILABLE:
        print("\nRunning hypothesis-based tests...")
        test_property_5_hypothesis()
    else:
        print("\n⚠️ Skipping hypothesis tests (hypothesis not installed)")

    print("\n✅ All Property 5 tests passed!")
