"""
System monitoring utilities for infrastructure

Features:
- Docker container stats
- PostgreSQL metrics
- Redis metrics
- System resources
"""

import json
import logging
import subprocess
from typing import Any

logger = logging.getLogger(__name__)


def get_docker_stats() -> list[dict[str, Any]]:
    """Get Docker container statistics"""
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "json"],
            capture_output=True,
            text=True,
            check=True,
        )

        stats = [json.loads(line) for line in result.stdout.strip().split("\n") if line]

        return stats

    except Exception as e:
        logger.error(f"Failed to get Docker stats: {e}")
        return []


def get_postgres_stats(conn_string: str) -> dict[str, Any]:
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
        cursor.execute(
            """
            SELECT pg_database_size(current_database()) as size
        """
        )
        stats["db_size_bytes"] = cursor.fetchone()[0]
        stats["db_size_mb"] = stats["db_size_bytes"] / 1024 / 1024

        # Connection count
        cursor.execute(
            """
            SELECT count(*) FROM pg_stat_activity
            WHERE datname = current_database()
        """
        )
        stats["active_connections"] = cursor.fetchone()[0]

        # Cache hit ratio
        cursor.execute(
            """
            SELECT
                sum(heap_blks_hit) / nullif(sum(heap_blks_hit) + sum(heap_blks_read), 0) * 100 as cache_hit_ratio
            FROM pg_statio_user_tables
        """
        )
        result = cursor.fetchone()
        stats["cache_hit_ratio"] = float(result[0]) if result[0] else 0.0

        # Transaction stats
        cursor.execute(
            """
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
        """
        )
        row = cursor.fetchone()
        if row:
            stats.update(
                {
                    "commits": row[0],
                    "rollbacks": row[1],
                    "blocks_read": row[2],
                    "blocks_hit": row[3],
                    "rows_returned": row[4],
                    "rows_fetched": row[5],
                    "rows_inserted": row[6],
                    "rows_updated": row[7],
                    "rows_deleted": row[8],
                }
            )

        cursor.close()
        conn.close()

        return stats

    except Exception as e:
        logger.error(f"Failed to get PostgreSQL stats: {e}")
        return {}


def get_redis_stats(redis_host: str = "localhost", redis_port: int = 6379) -> dict[str, Any]:
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
            "used_memory_mb": info["used_memory"] / 1024 / 1024,
            "used_memory_peak_mb": info["used_memory_peak"] / 1024 / 1024,
            "connected_clients": info["connected_clients"],
            "total_connections_received": info["total_connections_received"],
            "total_commands_processed": info["total_commands_processed"],
            "keyspace_hits": info.get("keyspace_hits", 0),
            "keyspace_misses": info.get("keyspace_misses", 0),
            "evicted_keys": info.get("evicted_keys", 0),
        }

        # Calculate hit rate
        total = stats["keyspace_hits"] + stats["keyspace_misses"]
        stats["hit_rate"] = (stats["keyspace_hits"] / total * 100) if total > 0 else 0.0

        return stats

    except Exception as e:
        logger.error(f"Failed to get Redis stats: {e}")
        return {}


def get_system_resources() -> dict[str, Any]:
    """Get system resource usage"""
    try:
        import psutil

        return {
            "cpu_percent": psutil.cpu_percent(interval=1),
            "cpu_count": psutil.cpu_count(),
            "memory_percent": psutil.virtual_memory().percent,
            "memory_available_mb": psutil.virtual_memory().available / 1024 / 1024,
            "disk_usage_percent": psutil.disk_usage("/").percent,
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
        print(
            f"   {stat.get('Name', 'Unknown')}: CPU {stat.get('CPUPerc', 'N/A')}, Memory {stat.get('MemPerc', 'N/A')}"
        )
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
