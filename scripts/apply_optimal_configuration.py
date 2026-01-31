#!/usr/bin/env python3
"""
Apply Optimal Configuration Script

This script analyzes system capabilities and generates optimal configuration
recommendations for the Tiki data pipeline.
"""

import sys
import os
import json
from pathlib import Path
from datetime import datetime

# Add src to path
src_path = Path(__file__).parent.parent / "src"
if src_path.exists():
    sys.path.insert(0, str(src_path))


def analyze_and_recommend():
    """Analyze system and generate configuration recommendations"""
    print("\n" + "=" * 80)
    print("CONFIGURATION OPTIMIZER")
    print("=" * 80)
    
    try:
        from common.configuration_analyzer import ConfigurationAnalyzer
        
        print("\n📊 Analyzing system capabilities...")
        analyzer = ConfigurationAnalyzer()
        
        # Analyze system
        system_caps = analyzer.analyze_system()
        
        print("\n✅ System Analysis Complete:")
        print(f"  - CPU Cores (Physical): {system_caps.cpu_cores_physical}")
        print(f"  - CPU Cores (Logical): {system_caps.cpu_cores_logical}")
        print(f"  - Available Memory: {system_caps.available_memory_mb / 1024:.2f} GB")
        print(f"  - Total Memory: {system_caps.total_memory_mb / 1024:.2f} GB")
        print(f"  - Disk Read Speed: {system_caps.disk_read_speed_mbps:.2f} MB/s")
        print(f"  - Disk Write Speed: {system_caps.disk_write_speed_mbps:.2f} MB/s")
        
        print("\n🔧 Generating optimal configuration...")
        config = analyzer.recommend_config(system_caps, profile_data=None)
        
        print("\n" + "=" * 80)
        print("RECOMMENDED CONFIGURATION")
        print("=" * 80)
        
        # Crawl configuration
        print("\n📡 CRAWL CONFIGURATION:")
        print(f"  PRODUCT_BATCH_SIZE: {config.product_batch_size}")
        print(f"  PRODUCT_POOL_SIZE: {config.product_pool_size}")
        print(f"  HTTP_CONNECTOR_LIMIT: {config.http_connector_limit}")
        print(f"  HTTP_TIMEOUT_TOTAL: {config.http_timeout_total}s")
        print(f"  HTTP_TIMEOUT_CONNECT: {config.http_timeout_connect}s")
        
        # Transform configuration
        print("\n🔄 TRANSFORM CONFIGURATION:")
        print(f"  THREAD_POOL_SIZE: {config.thread_pool_size}")
        print(f"  PROCESS_POOL_SIZE: {config.process_pool_size}")
        print(f"  USE_VECTORIZATION: {config.use_vectorization}")
        
        # Load configuration
        print("\n💾 LOAD CONFIGURATION:")
        print(f"  DB_CONNECTION_POOL_MIN: {config.db_connection_pool_min}")
        print(f"  DB_CONNECTION_POOL_MAX: {config.db_connection_pool_max}")
        print(f"  DB_BATCH_SIZE: {config.db_batch_size}")
        print(f"  USE_COPY_COMMAND: {config.use_copy_command}")
        
        # Cache configuration
        print("\n🗄️  CACHE CONFIGURATION:")
        print(f"  REDIS_CACHE_TTL: {config.redis_cache_ttl}s")
        print(f"  MEMORY_CACHE_SIZE_MB: {config.memory_cache_size_mb}MB")
        print(f"  CACHE_EVICTION_THRESHOLD: {config.cache_eviction_threshold}")
        
        # Rationale
        print("\n📝 RATIONALE:")
        for key, reason in config.rationale.items():
            print(f"  {key}: {reason}")
        
        # Generate Airflow variables commands
        print("\n" + "=" * 80)
        print("AIRFLOW VARIABLES COMMANDS")
        print("=" * 80)
        print("\nCopy and paste these commands to apply configuration:\n")
        
        commands = [
            f"airflow variables set TIKI_SAVE_BATCH_SIZE {config.db_batch_size}",
            f"airflow variables set TIKI_DETAIL_BATCH_SIZE {config.product_batch_size}",
            f"airflow variables set TIKI_CATEGORY_NUM_BATCHES {config.product_pool_size}",
            f"airflow variables set POSTGRES_POOL_SIZE {config.db_connection_pool_max}",
            f"airflow variables set HTTP_CONNECTOR_LIMIT {config.http_connector_limit}",
            f"airflow variables set HTTP_TIMEOUT_TOTAL {config.http_timeout_total}",
            f"airflow variables set HTTP_TIMEOUT_CONNECT {config.http_timeout_connect}",
            f"airflow variables set THREAD_POOL_SIZE {config.thread_pool_size}",
            f"airflow variables set REDIS_CACHE_TTL {config.redis_cache_ttl}",
        ]
        
        for cmd in commands:
            print(cmd)
        
        # Save to file
        output_dir = Path(".kiro/specs/performance-optimization")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / "OPTIMAL_CONFIGURATION.json"
        
        config_data = {
            "generated_at": datetime.now().isoformat(),
            "system_capabilities": {
                "cpu_cores_physical": system_caps.cpu_cores_physical,
                "cpu_cores_logical": system_caps.cpu_cores_logical,
                "total_memory_mb": system_caps.total_memory_mb,
                "available_memory_mb": system_caps.available_memory_mb,
                "disk_read_speed_mbps": system_caps.disk_read_speed_mbps,
                "disk_write_speed_mbps": system_caps.disk_write_speed_mbps,
            },
            "recommended_config": {
                "product_batch_size": config.product_batch_size,
                "product_pool_size": config.product_pool_size,
                "http_connector_limit": config.http_connector_limit,
                "http_timeout_total": config.http_timeout_total,
                "http_timeout_connect": config.http_timeout_connect,
                "thread_pool_size": config.thread_pool_size,
                "process_pool_size": config.process_pool_size,
                "use_vectorization": config.use_vectorization,
                "db_connection_pool_min": config.db_connection_pool_min,
                "db_connection_pool_max": config.db_connection_pool_max,
                "db_batch_size": config.db_batch_size,
                "use_copy_command": config.use_copy_command,
                "redis_cache_ttl": config.redis_cache_ttl,
                "memory_cache_size_mb": config.memory_cache_size_mb,
                "cache_eviction_threshold": config.cache_eviction_threshold,
            },
            "rationale": config.rationale,
            "airflow_commands": commands,
        }
        
        with open(output_file, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        print(f"\n✅ Configuration saved to: {output_file}")
        
        # Generate shell script
        shell_script = output_dir / "apply_configuration.sh"
        with open(shell_script, 'w', encoding='utf-8') as f:
            f.write("#!/bin/bash\n")
            f.write("# Auto-generated configuration script\n")
            f.write(f"# Generated: {datetime.now().isoformat()}\n\n")
            f.write("echo 'Applying optimal configuration...'\n\n")
            for cmd in commands:
                f.write(f"{cmd}\n")
            f.write("\necho 'Configuration applied!'\n")
            f.write("echo 'Remember to restart Airflow scheduler!'\n")
        
        # Make executable
        os.chmod(shell_script, 0o755)
        
        print(f"✅ Shell script saved to: {shell_script}")
        print("\nYou can also run:")
        print(f"  bash {shell_script}")
        
        print("\n" + "=" * 80)
        print("NEXT STEPS")
        print("=" * 80)
        print("\n1. Review the recommendations above")
        print("2. Apply configuration using one of these methods:")
        print("   a. Copy-paste the airflow commands above")
        print(f"   b. Run: bash {shell_script}")
        print("   c. Manually set in Airflow UI (Admin > Variables)")
        print("\n3. Restart Airflow scheduler:")
        print("   airflow scheduler restart")
        print("\n4. Run DAG and monitor performance improvements")
        
        return True
        
    except ImportError as e:
        print(f"\n❌ Error: Configuration analyzer not available: {e}")
        print("\nMake sure the optimization components are installed:")
        print("  pip install psutil")
        return False
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Main entry point"""
    success = analyze_and_recommend()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
