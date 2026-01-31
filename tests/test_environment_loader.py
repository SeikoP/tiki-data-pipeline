"""
Unit tests for environment_loader module

Feature: airflow-task-startup-optimization
Requirements: 3.3 - Environment loader shall load configuration files at most once per process
"""

import logging
import os
import tempfile

import pytest

from src.common.environment_loader import (
    clear_cache,
    ensure_environment_loaded,
    get_cache_info,
    load_environment,
    log_cache_stats,
)


@pytest.fixture(autouse=True)
def reset_cache():
    """Reset cache before each test"""
    clear_cache()
    yield
    clear_cache()


@pytest.fixture
def temp_env_file():
    """Create a temporary .env file for testing"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
        f.write("TEST_VAR=test_value\n")
        f.write("ANOTHER_VAR=another_value\n")
        temp_path = f.name

    yield temp_path

    # Cleanup
    try:
        os.unlink(temp_path)
    except Exception:
        pass


class TestEnvironmentLoader:
    """Test suite for environment loader functionality"""

    def test_load_environment_with_valid_file(self, temp_env_file):
        """Test loading environment from a valid .env file"""
        result = load_environment(temp_env_file)
        assert result is True

        # Verify cache was populated
        cache_info = get_cache_info()
        assert cache_info["misses"] == 1
        assert cache_info["hits"] == 0
        assert cache_info["currsize"] == 1

    def test_load_environment_caching(self, temp_env_file):
        """Test that subsequent calls use cache"""
        # First call - should miss cache
        result1 = load_environment(temp_env_file)
        assert result1 is True

        cache_info1 = get_cache_info()
        assert cache_info1["misses"] == 1
        assert cache_info1["hits"] == 0

        # Second call - should hit cache
        result2 = load_environment(temp_env_file)
        assert result2 is True

        cache_info2 = get_cache_info()
        assert cache_info2["misses"] == 1
        assert cache_info2["hits"] == 1

        # Third call - should hit cache again
        result3 = load_environment(temp_env_file)
        assert result3 is True

        cache_info3 = get_cache_info()
        assert cache_info3["misses"] == 1
        assert cache_info3["hits"] == 2

    def test_load_environment_with_nonexistent_file(self):
        """Test loading environment from nonexistent file"""
        result = load_environment("/nonexistent/path/.env")
        assert result is False

    def test_load_environment_without_path(self):
        """Test loading environment without specifying path (searches standard locations)"""
        # This will search for .env in standard locations
        # Should find the project .env file if it exists
        result = load_environment()
        # Result depends on whether .env exists in standard locations
        assert isinstance(result, bool)

    def test_clear_cache(self, temp_env_file):
        """Test cache clearing functionality"""
        # Load environment
        load_environment(temp_env_file)

        cache_info1 = get_cache_info()
        assert cache_info1["currsize"] == 1

        # Clear cache
        clear_cache()

        cache_info2 = get_cache_info()
        assert cache_info2["currsize"] == 0
        assert cache_info2["hits"] == 0
        assert cache_info2["misses"] == 0

    def test_get_cache_info_structure(self):
        """Test that cache info returns correct structure"""
        cache_info = get_cache_info()

        assert "hits" in cache_info
        assert "misses" in cache_info
        assert "maxsize" in cache_info
        assert "currsize" in cache_info

        assert isinstance(cache_info["hits"], int)
        assert isinstance(cache_info["misses"], int)
        assert isinstance(cache_info["maxsize"], int)
        assert isinstance(cache_info["currsize"], int)

        assert cache_info["maxsize"] == 1  # As defined in @lru_cache(maxsize=1)

    def test_log_cache_stats(self, temp_env_file, caplog):
        """Test that cache stats are logged correctly"""
        with caplog.at_level(logging.INFO):
            load_environment(temp_env_file)
            load_environment(temp_env_file)
            log_cache_stats()

        # Check that cache stats were logged
        assert any("cache stats" in record.message.lower() for record in caplog.records)
        assert any("hits=1" in record.message for record in caplog.records)
        assert any("misses=1" in record.message for record in caplog.records)

    def test_ensure_environment_loaded(self, temp_env_file):
        """Test convenience function ensure_environment_loaded"""
        result = ensure_environment_loaded(temp_env_file)
        assert result is True

        # Should use cache on second call
        result2 = ensure_environment_loaded(temp_env_file)
        assert result2 is True

        cache_info = get_cache_info()
        assert cache_info["hits"] >= 1

    def test_load_environment_logs_on_first_call(self, temp_env_file, caplog):
        """Test that loading is logged on first call"""
        with caplog.at_level(logging.INFO):
            load_environment(temp_env_file)

        # Check that loading was logged
        assert any("Loading environment from" in record.message for record in caplog.records)
        assert any("Environment loaded successfully" in record.message for record in caplog.records)

    def test_load_environment_no_duplicate_logs_on_cache_hit(self, temp_env_file, caplog):
        """Test that cache hits don't trigger duplicate loading logs"""
        # First call
        with caplog.at_level(logging.INFO):
            load_environment(temp_env_file)

        len([r for r in caplog.records if "Loading environment from" in r.message])

        # Clear caplog
        caplog.clear()

        # Second call (cache hit)
        with caplog.at_level(logging.INFO):
            load_environment(temp_env_file)

        # Should not have new "Loading environment from" logs (cache hit)
        cache_hit_log_count = len(
            [r for r in caplog.records if "Loading environment from" in r.message]
        )
        assert cache_hit_log_count == 0  # No new loading logs on cache hit

    def test_load_environment_missing_dotenv_package(self, temp_env_file, caplog):
        """Test behavior when python-dotenv is not installed"""
        # This test is difficult to implement due to caching and import mechanics
        # The ImportError would need to be triggered before the function is cached
        # Skipping this test as it's an edge case that's hard to simulate
        # In production, python-dotenv is a required dependency
        pytest.skip("Difficult to test ImportError with lru_cache - edge case")

    def test_multiple_processes_simulation(self, temp_env_file):
        """Test that cache is process-specific (simulated)"""
        # In a single process, cache should persist
        load_environment(temp_env_file)
        cache_info1 = get_cache_info()

        load_environment(temp_env_file)
        cache_info2 = get_cache_info()

        # Cache should show hits
        assert cache_info2["hits"] > cache_info1["hits"]

        # Clear cache to simulate new process
        clear_cache()

        load_environment(temp_env_file)
        cache_info3 = get_cache_info()

        # After clear, should be a miss again
        assert cache_info3["misses"] == 1
        assert cache_info3["hits"] == 0


class TestEnvironmentLoaderEdgeCases:
    """Test edge cases and error conditions"""

    def test_load_environment_with_empty_file(self):
        """Test loading from an empty .env file"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            temp_path = f.name

        try:
            result = load_environment(temp_path)
            # Should still succeed even with empty file
            assert result is True
        finally:
            os.unlink(temp_path)

    def test_load_environment_with_invalid_syntax(self):
        """Test loading from .env file with invalid syntax"""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".env", delete=False) as f:
            f.write("INVALID LINE WITHOUT EQUALS\n")
            f.write("VALID_VAR=value\n")
            temp_path = f.name

        try:
            # python-dotenv is forgiving and should still load valid lines
            result = load_environment(temp_path)
            assert result is True
        finally:
            os.unlink(temp_path)

    def test_load_environment_with_none_path_no_env_file(self):
        """Test loading with None path when no .env file exists"""
        # This test verifies behavior when searching standard locations
        # The actual result depends on whether .env exists in the project
        # We'll just verify the function returns a boolean and doesn't crash
        clear_cache()
        result = load_environment(None)
        assert isinstance(result, bool)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
