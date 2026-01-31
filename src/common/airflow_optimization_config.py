"""
Configuration management for Airflow task startup optimization.

This module provides a data class for managing Airflow optimization settings
and methods for converting to environment variables and validating configuration.
"""

from dataclasses import dataclass
from typing import Optional


@dataclass
class AirflowOptimizationConfig:
    """Configuration for Airflow optimization settings"""
    
    # Scheduler settings
    scheduler_heartbeat_sec: int = 5
    min_file_process_interval: int = 15
    dag_dir_list_interval: int = 30
    parsing_processes: int = 2
    
    # Database settings
    sql_alchemy_pool_size: int = 20
    sql_alchemy_max_overflow: int = 40
    sql_alchemy_pool_recycle: int = 3600
    sql_alchemy_pool_timeout: int = 30
    
    # Parallelism settings
    parallelism: int = 20
    max_active_tasks_per_dag: int = 10
    max_active_runs_per_dag: int = 3
    
    # DAG serialization settings
    store_serialized_dags: bool = True
    store_dag_code: bool = True
    min_serialized_dag_update_interval: int = 30
    
    def to_env_dict(self) -> dict:
        """Convert to environment variable dictionary"""
        return {
            'AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC': str(self.scheduler_heartbeat_sec),
            'AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL': str(self.min_file_process_interval),
            'AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL': str(self.dag_dir_list_interval),
            'AIRFLOW__SCHEDULER__PARSING_PROCESSES': str(self.parsing_processes),
            'AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE': str(self.sql_alchemy_pool_size),
            'AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW': str(self.sql_alchemy_max_overflow),
            'AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_RECYCLE': str(self.sql_alchemy_pool_recycle),
            'AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_TIMEOUT': str(self.sql_alchemy_pool_timeout),
            'AIRFLOW__CORE__PARALLELISM': str(self.parallelism),
            'AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG': str(self.max_active_tasks_per_dag),
            'AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG': str(self.max_active_runs_per_dag),
            'AIRFLOW__CORE__STORE_SERIALIZED_DAGS': str(self.store_serialized_dags),
            'AIRFLOW__CORE__STORE_DAG_CODE': str(self.store_dag_code),
            'AIRFLOW__CORE__MIN_SERIALIZED_DAG_UPDATE_INTERVAL': str(self.min_serialized_dag_update_interval),
        }
    
    def validate(self) -> tuple[bool, Optional[str]]:
        """Validate configuration values"""
        if self.scheduler_heartbeat_sec < 1:
            return False, "scheduler_heartbeat_sec must be >= 1"
        if self.min_file_process_interval < 1:
            return False, "min_file_process_interval must be >= 1"
        if self.dag_dir_list_interval < 1:
            return False, "dag_dir_list_interval must be >= 1"
        if self.parsing_processes < 1:
            return False, "parsing_processes must be >= 1"
        if self.sql_alchemy_pool_size < 1:
            return False, "sql_alchemy_pool_size must be >= 1"
        if self.sql_alchemy_max_overflow < 0:
            return False, "sql_alchemy_max_overflow must be >= 0"
        if self.sql_alchemy_pool_recycle < 1:
            return False, "sql_alchemy_pool_recycle must be >= 1"
        if self.sql_alchemy_pool_timeout < 1:
            return False, "sql_alchemy_pool_timeout must be >= 1"
        if self.parallelism < 1:
            return False, "parallelism must be >= 1"
        if self.max_active_tasks_per_dag < 1:
            return False, "max_active_tasks_per_dag must be >= 1"
        if self.max_active_runs_per_dag < 1:
            return False, "max_active_runs_per_dag must be >= 1"
        if self.min_serialized_dag_update_interval < 1:
            return False, "min_serialized_dag_update_interval must be >= 1"
        return True, None
