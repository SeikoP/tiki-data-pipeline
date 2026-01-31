"""
Example usage of ConfigurationAnalyzer to detect system capabilities
and generate optimized configuration recommendations.

This script demonstrates:
1. Auto-detecting system capabilities
2. Generating optimized configuration recommendations
3. Validating configuration values
4. Displaying recommendations with rationale
"""

import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from common.configuration_analyzer import ConfigurationAnalyzer


def main():
    """Demonstrate configuration analyzer usage."""
    print("=" * 80)
    print("Configuration Optimizer Example")
    print("=" * 80)
    print()
    
    # Create analyzer
    analyzer = ConfigurationAnalyzer()
    
    # Step 1: Detect system capabilities
    print("Step 1: Detecting system capabilities...")
    print("-" * 80)
    system = analyzer.analyze_system()
    
    print(f"CPU Cores:")
    print(f"  Physical: {system.cpu_cores_physical}")
    print(f"  Logical:  {system.cpu_cores_logical}")
    print(f"  Frequency: {system.cpu_freq_mhz:.0f} MHz")
    print()
    
    print(f"Memory:")
    print(f"  Total:     {system.total_memory_mb:.0f} MB")
    print(f"  Available: {system.available_memory_mb:.0f} MB")
    print()
    
    print(f"Disk I/O:")
    print(f"  Read:  {system.disk_read_speed_mbps:.1f} MB/s")
    print(f"  Write: {system.disk_write_speed_mbps:.1f} MB/s")
    print()
    
    print(f"Network:")
    print(f"  Bandwidth: {system.network_bandwidth_mbps:.1f} MB/s")
    print()
    
    print(f"Platform: {system.platform}")
    print(f"Python:   {system.python_version}")
    print()
    
    # Step 2: Generate optimized configuration
    print("Step 2: Generating optimized configuration...")
    print("-" * 80)
    config = analyzer.recommend_config(system)
    
    print("Crawl Configuration:")
    print(f"  product_batch_size:      {config.product_batch_size}")
    print(f"  product_pool_size:       {config.product_pool_size}")
    print(f"  http_connector_limit:    {config.http_connector_limit}")
    print(f"  http_timeout_total:      {config.http_timeout_total}s")
    print(f"  http_timeout_connect:    {config.http_timeout_connect}s")
    print()
    
    print("Transform Configuration:")
    print(f"  thread_pool_size:        {config.thread_pool_size}")
    print(f"  process_pool_size:       {config.process_pool_size}")
    print(f"  use_vectorization:       {config.use_vectorization}")
    print()
    
    print("Load Configuration:")
    print(f"  db_connection_pool_min:  {config.db_connection_pool_min}")
    print(f"  db_connection_pool_max:  {config.db_connection_pool_max}")
    print(f"  db_batch_size:           {config.db_batch_size}")
    print(f"  use_copy_command:        {config.use_copy_command}")
    print()
    
    print("Cache Configuration:")
    print(f"  redis_cache_ttl:         {config.redis_cache_ttl}s")
    print(f"  memory_cache_size_mb:    {config.memory_cache_size_mb} MB")
    print(f"  cache_eviction_threshold: {config.cache_eviction_threshold}")
    print()
    
    # Step 3: Validate configuration
    print("Step 3: Validating configuration...")
    print("-" * 80)
    is_valid, warnings = analyzer.validate_config(config, system)
    
    if is_valid:
        print("✅ Configuration is VALID")
    else:
        print("❌ Configuration is INVALID")
    
    if warnings:
        print(f"\nWarnings ({len(warnings)}):")
        for i, warning in enumerate(warnings, 1):
            print(f"  {i}. {warning}")
    else:
        print("\nNo warnings")
    print()
    
    # Step 4: Display rationale
    print("Step 4: Configuration Rationale")
    print("-" * 80)
    print("\nWhy these values were recommended:\n")
    
    for key, rationale in config.rationale.items():
        print(f"{key}:")
        print(f"  {rationale}")
        print()
    
    print("=" * 80)
    print("Configuration analysis complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
