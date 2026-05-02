"""
Example usage of AirflowOptimizationConfig.

This script demonstrates how to use the AirflowOptimizationConfig class
to manage Airflow optimization settings.
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.airflow_optimization_config import AirflowOptimizationConfig


def main():
    print("=" * 70)
    print("AIRFLOW OPTIMIZATION CONFIG EXAMPLE")
    print("=" * 70)

    # Example 1: Using default configuration
    print("\n1. Default Configuration:")
    print("-" * 70)
    config = AirflowOptimizationConfig()
    is_valid, error_msg = config.validate()
    print(f"Valid: {is_valid}")
    print(f"Scheduler Heartbeat: {config.scheduler_heartbeat_sec}s")
    print(f"Min File Process Interval: {config.min_file_process_interval}s")
    print(f"Pool Size: {config.sql_alchemy_pool_size}")
    print(f"Parallelism: {config.parallelism}")

    # Example 2: Custom configuration
    print("\n2. Custom Configuration:")
    print("-" * 70)
    custom_config = AirflowOptimizationConfig(
        scheduler_heartbeat_sec=3, parallelism=50, sql_alchemy_pool_size=30
    )
    is_valid, error_msg = custom_config.validate()
    print(f"Valid: {is_valid}")
    print(f"Scheduler Heartbeat: {custom_config.scheduler_heartbeat_sec}s")
    print(f"Parallelism: {custom_config.parallelism}")
    print(f"Pool Size: {custom_config.sql_alchemy_pool_size}")

    # Example 3: Converting to environment variables
    print("\n3. Environment Variables:")
    print("-" * 70)
    env_dict = config.to_env_dict()
    print(f"Total environment variables: {len(env_dict)}")
    print("\nSample environment variables:")
    for key in list(env_dict.keys())[:5]:
        print(f"  {key} = {env_dict[key]}")
    print("  ...")

    # Example 4: Invalid configuration
    print("\n4. Invalid Configuration Example:")
    print("-" * 70)
    invalid_config = AirflowOptimizationConfig(scheduler_heartbeat_sec=-1, parallelism=0)
    is_valid, error_msg = invalid_config.validate()
    print(f"Valid: {is_valid}")
    print(f"Error: {error_msg}")

    # Example 5: Boundary values
    print("\n5. Minimum Valid Configuration:")
    print("-" * 70)
    min_config = AirflowOptimizationConfig(
        scheduler_heartbeat_sec=1,
        min_file_process_interval=1,
        sql_alchemy_pool_size=1,
        parallelism=1,
    )
    is_valid, error_msg = min_config.validate()
    print(f"Valid: {is_valid}")
    print(f"All values set to minimum (1)")

    print("\n" + "=" * 70)
    print("EXAMPLE COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
