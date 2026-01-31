"""Unit tests for configuration analyzer system capability detection.

These tests verify specific functionality of the ConfigurationAnalyzer
including CPU detection, memory detection, and error handling.

Feature: performance-optimization
"""

import sys
from pathlib import Path
from unittest.mock import patch

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from common.configuration_analyzer import ConfigurationAnalyzer, SystemCapabilities


def test_cpu_core_detection():
    """Test CPU core detection.

    **Validates: Requirements 9.1**

    Verify that CPU cores (physical and logical) are detected correctly.
    """
    analyzer = ConfigurationAnalyzer()
    system = analyzer.analyze_system()

    # Verify CPU cores are detected
    assert system.cpu_cores_physical >= 1, "Should detect at least 1 physical core"
    assert system.cpu_cores_logical >= 1, "Should detect at least 1 logical core"
    assert system.cpu_cores_logical >= system.cpu_cores_physical, (
        "Logical cores should be >= physical cores"
    )

    # Verify CPU frequency is detected
    assert system.cpu_freq_mhz > 0, "CPU frequency should be positive"

    print(
        f"✅ Detected {system.cpu_cores_physical} physical cores, "
        f"{system.cpu_cores_logical} logical cores at {system.cpu_freq_mhz:.0f} MHz"
    )


def test_memory_detection():
    """Test memory detection.

    **Validates: Requirements 9.1**

    Verify that total and available memory are detected correctly.
    """
    analyzer = ConfigurationAnalyzer()
    system = analyzer.analyze_system()

    # Verify memory is detected
    assert system.total_memory_mb > 0, "Should detect total memory"
    assert system.available_memory_mb > 0, "Should detect available memory"
    assert system.available_memory_mb <= system.total_memory_mb, (
        "Available memory should not exceed total memory"
    )

    print(
        f"✅ Detected {system.total_memory_mb:.0f} MB total memory, "
        f"{system.available_memory_mb:.0f} MB available"
    )


def test_disk_io_speed_estimation():
    """Test disk I/O speed estimation.

    **Validates: Requirements 9.1**

    Verify that disk read/write speeds are estimated.
    """
    analyzer = ConfigurationAnalyzer()
    system = analyzer.analyze_system()

    # Verify disk speeds are estimated
    assert system.disk_read_speed_mbps > 0, "Should estimate disk read speed"
    assert system.disk_write_speed_mbps > 0, "Should estimate disk write speed"

    print(
        f"✅ Estimated disk I/O: read={system.disk_read_speed_mbps:.1f} MB/s, "
        f"write={system.disk_write_speed_mbps:.1f} MB/s"
    )


def test_network_bandwidth_estimation():
    """Test network bandwidth estimation.

    **Validates: Requirements 9.1**

    Verify that network bandwidth is estimated.
    """
    analyzer = ConfigurationAnalyzer()
    system = analyzer.analyze_system()

    # Verify network bandwidth is estimated
    assert system.network_bandwidth_mbps > 0, "Should estimate network bandwidth"

    print(f"✅ Estimated network bandwidth: {system.network_bandwidth_mbps:.1f} MB/s")


def test_platform_detection():
    """Test platform information detection.

    **Validates: Requirements 9.1**

    Verify that platform and Python version are detected.
    """
    analyzer = ConfigurationAnalyzer()
    system = analyzer.analyze_system()

    # Verify platform info is detected
    assert len(system.platform) > 0, "Should detect platform"
    assert len(system.python_version) > 0, "Should detect Python version"

    print(f"✅ Detected platform: {system.platform}, Python: {system.python_version}")


def test_detection_failure_handling():
    """Test handling of detection failures.

    **Validates: Requirements 9.1**

    Verify that the analyzer returns safe defaults when detection fails.
    """
    analyzer = ConfigurationAnalyzer()

    # Mock psutil to raise an exception
    with patch("common.configuration_analyzer.psutil") as mock_psutil:
        mock_psutil.cpu_count.side_effect = Exception("Mock error")
        mock_psutil.virtual_memory.side_effect = Exception("Mock error")
        mock_psutil.cpu_freq.side_effect = Exception("Mock error")

        system = analyzer.analyze_system()

        # Should return default capabilities
        assert system.cpu_cores_physical >= 1, "Should have default CPU cores"
        assert system.total_memory_mb > 0, "Should have default memory"
        assert system.disk_read_speed_mbps > 0, "Should have default disk speed"
        assert system.network_bandwidth_mbps > 0, "Should have default network bandwidth"

        print("✅ Detection failure handled gracefully with defaults")


def test_disk_io_estimation_failure_handling():
    """Test handling of disk I/O estimation failures.

    **Validates: Requirements 9.1**

    Verify that disk I/O estimation returns defaults on failure.
    """
    analyzer = ConfigurationAnalyzer()

    # Mock Path.write_bytes to raise an exception
    with patch("pathlib.Path.write_bytes", side_effect=Exception("Mock disk error")):
        read_speed, write_speed = analyzer._estimate_disk_io_speed()

        # Should return default speeds
        assert read_speed > 0, "Should have default read speed"
        assert write_speed > 0, "Should have default write speed"

        print(
            f"✅ Disk I/O estimation failure handled: "
            f"read={read_speed} MB/s, write={write_speed} MB/s"
        )


def test_network_estimation_failure_handling():
    """Test handling of network bandwidth estimation failures.

    **Validates: Requirements 9.1**

    Verify that network estimation returns defaults on failure.
    """
    analyzer = ConfigurationAnalyzer()

    # Mock psutil.net_if_stats to raise an exception
    with patch(
        "common.configuration_analyzer.psutil.net_if_stats",
        side_effect=Exception("Mock network error"),
    ):
        bandwidth = analyzer._estimate_network_bandwidth()

        # Should return default bandwidth
        assert bandwidth > 0, "Should have default bandwidth"

        print(f"✅ Network estimation failure handled: {bandwidth} MB/s")


def test_config_generation_with_detected_system():
    """Test configuration generation with detected system.

    **Validates: Requirements 9.1, 9.2**

    Verify that configuration can be generated from detected system capabilities.
    """
    analyzer = ConfigurationAnalyzer()

    # Auto-detect system
    system = analyzer.analyze_system()

    # Generate config
    config = analyzer.recommend_config(system)

    # Verify config is valid
    assert config.product_batch_size > 0
    assert config.product_pool_size > 0
    assert config.http_connector_limit > 0
    assert len(config.rationale) > 0

    # Validate config
    is_valid, warnings = analyzer.validate_config(config, system)
    assert is_valid, f"Config should be valid. Warnings: {warnings}"

    print("✅ Configuration generated successfully from detected system")


def test_config_generation_without_system():
    """Test configuration generation without providing system.

    **Validates: Requirements 9.1, 9.2**

    Verify that configuration can be generated with auto-detection.
    """
    analyzer = ConfigurationAnalyzer()

    # Generate config without providing system (should auto-detect)
    config = analyzer.recommend_config()

    # Verify config is valid
    assert config.product_batch_size > 0
    assert config.product_pool_size > 0
    assert len(config.rationale) > 0

    print("✅ Configuration generated successfully with auto-detection")


def test_validation_without_system():
    """Test validation without providing system.

    **Validates: Requirements 9.1, 9.3**

    Verify that validation can auto-detect system if not provided.
    """
    analyzer = ConfigurationAnalyzer()

    # Create a valid config
    config = analyzer.recommend_config()

    # Validate without providing system (should auto-detect)
    is_valid, warnings = analyzer.validate_config(config)

    assert is_valid, f"Config should be valid. Warnings: {warnings}"

    print("✅ Validation works with auto-detection")


def test_rationale_completeness():
    """Test that rationale is provided for all configuration values.

    **Validates: Requirements 9.2, 9.7**

    Verify that every configuration value has a rationale.
    """
    analyzer = ConfigurationAnalyzer()
    config = analyzer.recommend_config()

    # Expected rationale keys
    expected_keys = [
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

    for key in expected_keys:
        assert key in config.rationale, f"Missing rationale for {key}"
        assert len(config.rationale[key]) > 0, f"Empty rationale for {key}"
        assert isinstance(config.rationale[key], str), f"Rationale for {key} should be string"

    print(f"✅ All {len(expected_keys)} configuration values have rationale")


def test_config_scales_with_resources():
    """Test that configuration scales appropriately with system resources.

    **Validates: Requirements 9.2, 9.3, 9.4, 9.5, 9.6**

    Verify that higher resource systems get larger configuration values.
    """
    analyzer = ConfigurationAnalyzer()

    # Low-resource system
    low_system = SystemCapabilities(
        cpu_cores_physical=2,
        cpu_cores_logical=4,
        cpu_freq_mhz=1500.0,
        total_memory_mb=2048.0,
        available_memory_mb=1024.0,
        disk_read_speed_mbps=50.0,
        disk_write_speed_mbps=30.0,
        network_bandwidth_mbps=5.0,
        platform="Linux",
        python_version="3.11.0",
    )

    # High-resource system
    high_system = SystemCapabilities(
        cpu_cores_physical=16,
        cpu_cores_logical=32,
        cpu_freq_mhz=3500.0,
        total_memory_mb=32768.0,
        available_memory_mb=16384.0,
        disk_read_speed_mbps=500.0,
        disk_write_speed_mbps=400.0,
        network_bandwidth_mbps=100.0,
        platform="Linux",
        python_version="3.11.0",
    )

    low_config = analyzer.recommend_config(low_system)
    high_config = analyzer.recommend_config(high_system)

    # High-resource system should have larger values
    assert high_config.product_batch_size >= low_config.product_batch_size, (
        "Higher memory should allow larger batch sizes"
    )
    assert high_config.product_pool_size >= low_config.product_pool_size, (
        "More CPU cores should allow larger pool sizes"
    )
    assert high_config.thread_pool_size >= low_config.thread_pool_size, (
        "More CPU cores should allow larger thread pools"
    )
    assert high_config.process_pool_size >= low_config.process_pool_size, (
        "More CPU cores should allow larger process pools"
    )
    assert high_config.http_connector_limit >= low_config.http_connector_limit, (
        "Higher bandwidth should allow more connections"
    )
    assert high_config.db_batch_size >= low_config.db_batch_size, (
        "Faster disk should allow larger batch sizes"
    )
    assert high_config.memory_cache_size_mb >= low_config.memory_cache_size_mb, (
        "More memory should allow larger cache"
    )

    print("✅ Configuration scales appropriately with system resources")


if __name__ == "__main__":
    print("Running unit tests for system capability detection...\n")

    test_cpu_core_detection()
    test_memory_detection()
    test_disk_io_speed_estimation()
    test_network_bandwidth_estimation()
    test_platform_detection()
    test_detection_failure_handling()
    test_disk_io_estimation_failure_handling()
    test_network_estimation_failure_handling()
    test_config_generation_with_detected_system()
    test_config_generation_without_system()
    test_validation_without_system()
    test_rationale_completeness()
    test_config_scales_with_resources()

    print("\n✅ All unit tests passed!")
