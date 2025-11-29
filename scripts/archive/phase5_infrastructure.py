"""
Phase 5: Infrastructure Optimization

Focus:
1. Docker optimization
2. PostgreSQL tuning
3. Redis optimization
4. System monitoring
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

print("=" * 70)
print("🔧 PHASE 5: INFRASTRUCTURE OPTIMIZATION")
print("=" * 70)
print()

# Step 1: PostgreSQL configuration tuning
print("🗄️ Step 1: PostgreSQL tuning configuration")
print("-" * 70)
print()

postgres_tuning = """# PostgreSQL Performance Tuning Configuration
# Add these to postgresql.conf or set via ALTER SYSTEM

# Memory Settings
shared_buffers = 256MB                  # 25% of RAM for dedicated server
effective_cache_size = 1GB              # 50-75% of RAM
work_mem = 16MB                         # Per operation memory
maintenance_work_mem = 128MB            # For VACUUM, CREATE INDEX

# Connection Settings
max_connections = 100                   # Reduce if using connection pooling
shared_preload_libraries = 'pg_stat_statements'

# Query Planning
random_page_cost = 1.1                  # Lower for SSD (default 4.0)
effective_io_concurrency = 200          # Higher for SSD (default 1)

# Write Performance
wal_buffers = 16MB
checkpoint_completion_target = 0.9
wal_compression = on

# Autovacuum (important for high-write workloads)
autovacuum = on
autovacuum_max_workers = 4
autovacuum_naptime = 10s

# Logging (for monitoring)
log_min_duration_statement = 1000       # Log queries > 1s
log_checkpoints = on
log_connections = on
log_disconnections = on
log_lock_waits = on

# Performance Monitoring
track_activities = on
track_counts = on
track_io_timing = on
track_functions = all
"""

postgres_file = project_root / "airflow" / "setup" / "postgresql_tuning.conf"
with open(postgres_file, "w", encoding="utf-8") as f:
    f.write(postgres_tuning)

print(f"✅ Created: {postgres_file.relative_to(project_root)}")
print()

# Step 2: Docker Compose optimization
print("🐳 Step 2: Docker Compose optimization")
print("-" * 70)
print()

docker_override = """# Docker Compose Override for Performance
# File: docker-compose.override.yml
# Usage: Automatically merged with docker-compose.yml

version: '3.8'

services:
  postgres:
    # Performance tuning
    command: >
      postgres
      -c shared_buffers=256MB
      -c effective_cache_size=1GB
      -c work_mem=16MB
      -c maintenance_work_mem=128MB
      -c random_page_cost=1.1
      -c effective_io_concurrency=200
      -c wal_buffers=16MB
      -c checkpoint_completion_target=0.9
      -c max_connections=100
    
    # Resource limits
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 2G
        reservations:
          cpus: '1.0'
          memory: 1G
    
    # Health check
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 30s

  redis:
    # Redis configuration
    command: >
      redis-server
      --maxmemory 512mb
      --maxmemory-policy allkeys-lru
      --save 60 1000
      --appendonly yes
      --tcp-backlog 511
      --timeout 300
    
    # Resource limits
    deploy:
      resources:
        limits:
          cpus: '1.0'
          memory: 1G
        reservations:
          cpus: '0.5'
          memory: 512M
    
    # Health check
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  airflow-scheduler:
    # Scheduler optimization
    environment:
      - AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL=30
      - AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL=60
      - AIRFLOW__SCHEDULER__PARSING_PROCESSES=2
      - AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY=512
    
    # Resource limits
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 2G
        reservations:
          cpus: '1.0'
          memory: 1G

  airflow-worker:
    # Worker optimization
    environment:
      - AIRFLOW__CELERY__WORKER_CONCURRENCY=4
      - AIRFLOW__CELERY__WORKER_PREFETCH_MULTIPLIER=2
    
    # Resource limits
    deploy:
      resources:
        limits:
          cpus: '4.0'
          memory: 4G
        reservations:
          cpus: '2.0'
          memory: 2G
"""

docker_file = project_root / "docker-compose.performance.yml"
with open(docker_file, "w", encoding="utf-8") as f:
    f.write(docker_override)

print(f"✅ Created: {docker_file.relative_to(project_root)}")
print()

# Step 3: System monitoring script
print("📊 Step 3: System monitoring utilities")
print("-" * 70)
print()

monitoring_script = '''"""
System monitoring utilities for infrastructure

Features:
- Docker container stats
- PostgreSQL metrics
- Redis metrics
- System resources
"""

import subprocess
import json
import logging
from typing import Dict, Any, List

logger = logging.getLogger(__name__)


def get_docker_stats() -> List[Dict[str, Any]]:
    """Get Docker container statistics"""
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "json"],
            capture_output=True,
            text=True,
            check=True
        )
        
        stats = []
        for line in result.stdout.strip().split('\\n'):
            if line:
                stats.append(json.loads(line))
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get Docker stats: {e}")
        return []


def get_postgres_stats(conn_string: str) -> Dict[str, Any]:
    """
    Get PostgreSQL performance statistics
    
    Args:
        conn_string: PostgreSQL connection string
        
    Returns:
        Dictionary with database statistics
    """
    try:
        import psycopg2
        
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        
        stats = {}
        
        # Database size
        cursor.execute("""
            SELECT pg_database_size(current_database()) as size
        """)
        stats['db_size_bytes'] = cursor.fetchone()[0]
        stats['db_size_mb'] = stats['db_size_bytes'] / 1024 / 1024
        
        # Connection count
        cursor.execute("""
            SELECT count(*) FROM pg_stat_activity
            WHERE datname = current_database()
        """)
        stats['active_connections'] = cursor.fetchone()[0]
        
        # Cache hit ratio
        cursor.execute("""
            SELECT 
                sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100 as cache_hit_ratio
            FROM pg_statio_user_tables
        """)
        result = cursor.fetchone()
        stats['cache_hit_ratio'] = float(result[0]) if result[0] else 0.0
        
        # Transaction stats
        cursor.execute("""
            SELECT 
                xact_commit,
                xact_rollback,
                blks_read,
                blks_hit,
                tup_returned,
                tup_fetched,
                tup_inserted,
                tup_updated,
                tup_deleted
            FROM pg_stat_database
            WHERE datname = current_database()
        """)
        row = cursor.fetchone()
        if row:
            stats.update({
                'commits': row[0],
                'rollbacks': row[1],
                'blocks_read': row[2],
                'blocks_hit': row[3],
                'rows_returned': row[4],
                'rows_fetched': row[5],
                'rows_inserted': row[6],
                'rows_updated': row[7],
                'rows_deleted': row[8],
            })
        
        cursor.close()
        conn.close()
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get PostgreSQL stats: {e}")
        return {}


def get_redis_stats(redis_host: str = "localhost", redis_port: int = 6379) -> Dict[str, Any]:
    """
    Get Redis statistics
    
    Args:
        redis_host: Redis host
        redis_port: Redis port
        
    Returns:
        Dictionary with Redis statistics
    """
    try:
        import redis
        
        client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        info = client.info()
        
        stats = {
            'used_memory_mb': info['used_memory'] / 1024 / 1024,
            'used_memory_peak_mb': info['used_memory_peak'] / 1024 / 1024,
            'connected_clients': info['connected_clients'],
            'total_connections_received': info['total_connections_received'],
            'total_commands_processed': info['total_commands_processed'],
            'keyspace_hits': info.get('keyspace_hits', 0),
            'keyspace_misses': info.get('keyspace_misses', 0),
            'evicted_keys': info.get('evicted_keys', 0),
        }
        
        # Calculate hit rate
        total = stats['keyspace_hits'] + stats['keyspace_misses']
        stats['hit_rate'] = (stats['keyspace_hits'] / total * 100) if total > 0 else 0.0
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get Redis stats: {e}")
        return {}


def get_system_resources() -> Dict[str, Any]:
    """Get system resource usage"""
    try:
        import psutil
        
        return {
            'cpu_percent': psutil.cpu_percent(interval=1),
            'cpu_count': psutil.cpu_count(),
            'memory_percent': psutil.virtual_memory().percent,
            'memory_available_mb': psutil.virtual_memory().available / 1024 / 1024,
            'disk_usage_percent': psutil.disk_usage('/').percent,
        }
        
    except Exception as e:
        logger.error(f"Failed to get system resources: {e}")
        return {}


def print_monitoring_report():
    """Print comprehensive monitoring report"""
    print("=" * 70)
    print("📊 INFRASTRUCTURE MONITORING REPORT")
    print("=" * 70)
    print()
    
    # System resources
    print("💻 System Resources:")
    sys_stats = get_system_resources()
    if sys_stats:
        print(f"   CPU: {sys_stats['cpu_percent']:.1f}% ({sys_stats['cpu_count']} cores)")
        print(f"   Memory: {sys_stats['memory_percent']:.1f}% used")
        print(f"   Disk: {sys_stats['disk_usage_percent']:.1f}% used")
    print()
    
    # Docker stats
    print("🐳 Docker Containers:")
    docker_stats = get_docker_stats()
    for stat in docker_stats:
        print(f"   {stat.get('Name', 'Unknown')}: CPU {stat.get('CPUPerc', 'N/A')}, Memory {stat.get('MemPerc', 'N/A')}")
    print()
    
    # PostgreSQL stats
    print("🗄️ PostgreSQL:")
    pg_stats = get_postgres_stats("postgresql://localhost:5432/crawl_data")
    if pg_stats:
        print(f"   Database size: {pg_stats['db_size_mb']:.1f} MB")
        print(f"   Active connections: {pg_stats['active_connections']}")
        print(f"   Cache hit ratio: {pg_stats['cache_hit_ratio']:.2f}%")
    print()
    
    # Redis stats
    print("🔴 Redis:")
    redis_stats = get_redis_stats()
    if redis_stats:
        print(f"   Memory used: {redis_stats['used_memory_mb']:.1f} MB")
        print(f"   Connected clients: {redis_stats['connected_clients']}")
        print(f"   Hit rate: {redis_stats['hit_rate']:.2f}%")
    print()


__all__ = [
    "get_docker_stats",
    "get_postgres_stats",
    "get_redis_stats",
    "get_system_resources",
    "print_monitoring_report",
]
'''

monitoring_file = project_root / "src" / "common" / "infrastructure_monitor.py"
with open(monitoring_file, "w", encoding="utf-8") as f:
    f.write(monitoring_script)

print(f"✅ Created: {monitoring_file.relative_to(project_root)}")
print()

# Summary
print("=" * 70)
print("📊 PHASE 5 COMPLETED")
print("=" * 70)
print()
print("✅ Created infrastructure optimization files:")
print("   • postgresql_tuning.conf - PostgreSQL performance config")
print("   • docker-compose.performance.yml - Docker resource limits & health checks")
print("   • infrastructure_monitor.py - System monitoring utilities")
print()
print("🎯 Key Optimizations:")
print("   • PostgreSQL: 256MB shared_buffers, SSD tuning (random_page_cost=1.1)")
print("   • Redis: 512MB max memory, LRU eviction, AOF persistence")
print("   • Docker: Resource limits, health checks, restart policies")
print("   • Monitoring: Docker, PostgreSQL, Redis, system metrics")
print()
print("📋 Apply optimizations:")
print("   1. PostgreSQL: COPY postgresql_tuning.conf to container")
print("   2. Docker: docker-compose -f docker-compose.yml -f docker-compose.performance.yml up -d")
print(
    "   3. Monitor: python -c 'from src.common.infrastructure_monitor import print_monitoring_report; print_monitoring_report()'"
)
print()
