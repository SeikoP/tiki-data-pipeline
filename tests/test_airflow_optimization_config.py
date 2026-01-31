"""
Unit tests for AirflowOptimizationConfig data class.

Tests configuration validation, environment variable conversion,
and edge cases for the Airflow optimization configuration.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import pytest

from common.airflow_optimization_config import AirflowOptimizationConfig


class TestAirflowOptimizationConfig:
    """Test suite for AirflowOptimizationConfig"""

    def test_default_configuration_is_valid(self):
        """Test that default configuration values are valid"""
        config = AirflowOptimizationConfig()
        is_valid, error_msg = config.validate()

        assert is_valid is True
        assert error_msg is None

    def test_default_values(self):
        """Test that default values match specification"""
        config = AirflowOptimizationConfig()

        # Scheduler settings
        assert config.scheduler_heartbeat_sec == 5
        assert config.min_file_process_interval == 15
        assert config.dag_dir_list_interval == 30
        assert config.parsing_processes == 2

        # Database settings
        assert config.sql_alchemy_pool_size == 20
        assert config.sql_alchemy_max_overflow == 40
        assert config.sql_alchemy_pool_recycle == 3600
        assert config.sql_alchemy_pool_timeout == 30

        # Parallelism settings
        assert config.parallelism == 20
        assert config.max_active_tasks_per_dag == 10
        assert config.max_active_runs_per_dag == 3

        # DAG serialization settings
        assert config.store_serialized_dags is True
        assert config.store_dag_code is True
        assert config.min_serialized_dag_update_interval == 30

    def test_to_env_dict_returns_correct_keys(self):
        """Test that to_env_dict returns all expected environment variables"""
        config = AirflowOptimizationConfig()
        env_dict = config.to_env_dict()

        expected_keys = {
            "AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC",
            "AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL",
            "AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL",
            "AIRFLOW__SCHEDULER__PARSING_PROCESSES",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE",
            "AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_TIMEOUT",
            "AIRFLOW__CORE__PARALLELISM",
            "AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG",
            "AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG",
            "AIRFLOW__CORE__STORE_SERIALIZED_DAGS",
            "AIRFLOW__CORE__STORE_DAG_CODE",
            "AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL",
        }

        assert set(env_dict.keys()) == expected_keys

    def test_to_env_dict_values_are_strings(self):
        """Test that all environment variable values are strings"""
        config = AirflowOptimizationConfig()
        env_dict = config.to_env_dict()

        for key, value in env_dict.items():
            assert isinstance(value, str), f"{key} value should be string, got {type(value)}"

    def test_to_env_dict_boolean_conversion(self):
        """Test that boolean values are correctly converted to strings"""
        config = AirflowOptimizationConfig(store_serialized_dags=True, store_dag_code=False)
        env_dict = config.to_env_dict()

        assert env_dict["AIRFLOW__CORE__STORE_SERIALIZED_DAGS"] == "True"
        assert env_dict["AIRFLOW__CORE__STORE_DAG_CODE"] == "False"

    def test_to_env_dict_custom_values(self):
        """Test that custom configuration values are correctly converted"""
        config = AirflowOptimizationConfig(
            scheduler_heartbeat_sec=10, parallelism=50, sql_alchemy_pool_size=30
        )
        env_dict = config.to_env_dict()

        assert env_dict["AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC"] == "10"
        assert env_dict["AIRFLOW__CORE__PARALLELISM"] == "50"
        assert env_dict["AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE"] == "30"

    # Validation tests for scheduler settings
    def test_validate_negative_scheduler_heartbeat(self):
        """Test validation fails for negative scheduler_heartbeat_sec"""
        config = AirflowOptimizationConfig(scheduler_heartbeat_sec=-1)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "scheduler_heartbeat_sec must be >= 1"

    def test_validate_zero_scheduler_heartbeat(self):
        """Test validation fails for zero scheduler_heartbeat_sec"""
        config = AirflowOptimizationConfig(scheduler_heartbeat_sec=0)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "scheduler_heartbeat_sec must be >= 1"

    def test_validate_negative_min_file_process_interval(self):
        """Test validation fails for negative min_file_process_interval"""
        config = AirflowOptimizationConfig(min_file_process_interval=-5)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "min_file_process_interval must be >= 1"

    def test_validate_zero_min_file_process_interval(self):
        """Test validation fails for zero min_file_process_interval"""
        config = AirflowOptimizationConfig(min_file_process_interval=0)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "min_file_process_interval must be >= 1"

    def test_validate_negative_dag_dir_list_interval(self):
        """Test validation fails for negative dag_dir_list_interval"""
        config = AirflowOptimizationConfig(dag_dir_list_interval=-10)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "dag_dir_list_interval must be >= 1"

    def test_validate_negative_parsing_processes(self):
        """Test validation fails for negative parsing_processes"""
        config = AirflowOptimizationConfig(parsing_processes=-2)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "parsing_processes must be >= 1"

    # Validation tests for database settings
    def test_validate_negative_pool_size(self):
        """Test validation fails for negative sql_alchemy_pool_size"""
        config = AirflowOptimizationConfig(sql_alchemy_pool_size=-1)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "sql_alchemy_pool_size must be >= 1"

    def test_validate_zero_pool_size(self):
        """Test validation fails for zero sql_alchemy_pool_size"""
        config = AirflowOptimizationConfig(sql_alchemy_pool_size=0)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "sql_alchemy_pool_size must be >= 1"

    def test_validate_negative_max_overflow(self):
        """Test validation fails for negative sql_alchemy_max_overflow"""
        config = AirflowOptimizationConfig(sql_alchemy_max_overflow=-5)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "sql_alchemy_max_overflow must be >= 0"

    def test_validate_zero_max_overflow_is_valid(self):
        """Test validation succeeds for zero sql_alchemy_max_overflow"""
        config = AirflowOptimizationConfig(sql_alchemy_max_overflow=0)
        is_valid, error_msg = config.validate()

        assert is_valid is True
        assert error_msg is None

    def test_validate_negative_pool_recycle(self):
        """Test validation fails for negative sql_alchemy_pool_recycle"""
        config = AirflowOptimizationConfig(sql_alchemy_pool_recycle=-100)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "sql_alchemy_pool_recycle must be >= 1"

    def test_validate_negative_pool_timeout(self):
        """Test validation fails for negative sql_alchemy_pool_timeout"""
        config = AirflowOptimizationConfig(sql_alchemy_pool_timeout=-30)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "sql_alchemy_pool_timeout must be >= 1"

    # Validation tests for parallelism settings
    def test_validate_negative_parallelism(self):
        """Test validation fails for negative parallelism"""
        config = AirflowOptimizationConfig(parallelism=-10)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "parallelism must be >= 1"

    def test_validate_zero_parallelism(self):
        """Test validation fails for zero parallelism"""
        config = AirflowOptimizationConfig(parallelism=0)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "parallelism must be >= 1"

    def test_validate_negative_max_active_tasks_per_dag(self):
        """Test validation fails for negative max_active_tasks_per_dag"""
        config = AirflowOptimizationConfig(max_active_tasks_per_dag=-5)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "max_active_tasks_per_dag must be >= 1"

    def test_validate_negative_max_active_runs_per_dag(self):
        """Test validation fails for negative max_active_runs_per_dag"""
        config = AirflowOptimizationConfig(max_active_runs_per_dag=-3)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "max_active_runs_per_dag must be >= 1"

    def test_validate_negative_min_serialized_dag_update_interval(self):
        """Test validation fails for negative min_serialized_dag_update_interval"""
        config = AirflowOptimizationConfig(min_serialized_dag_update_interval=-15)
        is_valid, error_msg = config.validate()

        assert is_valid is False
        assert error_msg == "min_serialized_dag_update_interval must be >= 1"

    # Boundary condition tests
    def test_validate_minimum_valid_values(self):
        """Test validation succeeds with all minimum valid values (1)"""
        config = AirflowOptimizationConfig(
            scheduler_heartbeat_sec=1,
            min_file_process_interval=1,
            dag_dir_list_interval=1,
            parsing_processes=1,
            sql_alchemy_pool_size=1,
            sql_alchemy_max_overflow=0,  # 0 is valid for overflow
            sql_alchemy_pool_recycle=1,
            sql_alchemy_pool_timeout=1,
            parallelism=1,
            max_active_tasks_per_dag=1,
            max_active_runs_per_dag=1,
            min_serialized_dag_update_interval=1,
        )
        is_valid, error_msg = config.validate()

        assert is_valid is True
        assert error_msg is None

    def test_validate_large_values(self):
        """Test validation succeeds with very large values"""
        config = AirflowOptimizationConfig(
            scheduler_heartbeat_sec=1000,
            min_file_process_interval=5000,
            sql_alchemy_pool_size=1000,
            parallelism=10000,
        )
        is_valid, error_msg = config.validate()

        assert is_valid is True
        assert error_msg is None

    # Edge case tests
    def test_multiple_invalid_parameters_returns_first_error(self):
        """Test that validation returns the first error when multiple parameters are invalid"""
        config = AirflowOptimizationConfig(
            scheduler_heartbeat_sec=-1, parallelism=-10, sql_alchemy_pool_size=-5
        )
        is_valid, error_msg = config.validate()

        assert is_valid is False
        # Should return the first error encountered
        assert error_msg == "scheduler_heartbeat_sec must be >= 1"

    def test_config_immutability_after_validation(self):
        """Test that validation doesn't modify the configuration"""
        config = AirflowOptimizationConfig(scheduler_heartbeat_sec=10)
        original_value = config.scheduler_heartbeat_sec

        config.validate()

        assert config.scheduler_heartbeat_sec == original_value

    def test_to_env_dict_does_not_modify_config(self):
        """Test that to_env_dict doesn't modify the configuration"""
        config = AirflowOptimizationConfig(parallelism=25)
        original_parallelism = config.parallelism

        config.to_env_dict()

        assert config.parallelism == original_parallelism
        assert isinstance(config.parallelism, int)

    def test_config_with_all_custom_values(self):
        """Test configuration with all custom values"""
        config = AirflowOptimizationConfig(
            scheduler_heartbeat_sec=3,
            min_file_process_interval=10,
            dag_dir_list_interval=20,
            parsing_processes=4,
            sql_alchemy_pool_size=30,
            sql_alchemy_max_overflow=60,
            sql_alchemy_pool_recycle=7200,
            sql_alchemy_pool_timeout=60,
            parallelism=50,
            max_active_tasks_per_dag=20,
            max_active_runs_per_dag=5,
            store_serialized_dags=False,
            store_dag_code=False,
            min_serialized_dag_update_interval=60,
        )

        is_valid, error_msg = config.validate()
        assert is_valid is True
        assert error_msg is None

        env_dict = config.to_env_dict()
        assert env_dict["AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC"] == "3"
        assert env_dict["AIRFLOW__CORE__PARALLELISM"] == "50"
        assert env_dict["AIRFLOW__CORE__STORE_SERIALIZED_DAGS"] == "False"


def test_config_can_be_instantiated():
    """Test that AirflowOptimizationConfig can be instantiated"""
    config = AirflowOptimizationConfig()
    assert config is not None
    assert isinstance(config, AirflowOptimizationConfig)


def test_config_is_dataclass():
    """Test that AirflowOptimizationConfig is a dataclass"""
    from dataclasses import is_dataclass

    assert is_dataclass(AirflowOptimizationConfig)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
