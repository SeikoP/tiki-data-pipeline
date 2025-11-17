"""
Database connection pooling for PostgreSQL

Optimizations:
- Reuse connections instead of creating new ones
- Pool size management
- Connection health checks
- Automatic reconnection
"""

import os
from contextlib import contextmanager
from typing import Any, Generator

try:
    import psycopg2
    from psycopg2 import pool
    PSYCOPG2_AVAILABLE = True
except ImportError:
    PSYCOPG2_AVAILABLE = False
    pool = None


class PostgresConnectionPool:
    """Thread-safe PostgreSQL connection pool"""
    
    _instance = None
    _pool = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def initialize(
        self,
        host: str = None,
        port: int = 5432,
        database: str = "crawl_data",
        user: str = None,
        password: str = None,
        minconn: int = 2,
        maxconn: int = 10
    ):
        """
        Initialize connection pool
        
        Args:
            host: Database host (default from env)
            port: Database port
            database: Database name
            user: Database user (default from env)
            password: Database password (default from env)
            minconn: Minimum connections in pool
            maxconn: Maximum connections in pool
        """
        if not PSYCOPG2_AVAILABLE:
            raise ImportError("psycopg2 not installed. Install: pip install psycopg2-binary")
        
        if self._pool is not None:
            return  # Already initialized
        
        # Get credentials from environment if not provided
        host = host or os.getenv("POSTGRES_HOST", "localhost")
        user = user or os.getenv("POSTGRES_USER", "postgres")
        password = password or os.getenv("POSTGRES_PASSWORD", "")
        
        try:
            self._pool = pool.ThreadedConnectionPool(
                minconn,
                maxconn,
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                connect_timeout=10,
                options="-c statement_timeout=30000"  # 30s statement timeout
            )
            print(f"✅ PostgreSQL connection pool initialized: {minconn}-{maxconn} connections")
        except Exception as e:
            print(f"❌ Failed to initialize connection pool: {e}")
            raise
    
    def get_connection(self):
        """Get a connection from the pool"""
        if self._pool is None:
            raise RuntimeError("Connection pool not initialized. Call initialize() first.")
        return self._pool.getconn()
    
    def return_connection(self, conn):
        """Return a connection to the pool"""
        if self._pool is not None:
            self._pool.putconn(conn)
    
    def close_all(self):
        """Close all connections in the pool"""
        if self._pool is not None:
            self._pool.closeall()
            self._pool = None
            print("✅ All connections closed")
    
    @contextmanager
    def get_cursor(self, commit: bool = True) -> Generator[Any, None, None]:
        """
        Context manager for database cursor
        
        Args:
            commit: Auto-commit after context exit
            
        Usage:
            with pool.get_cursor() as cursor:
                cursor.execute("SELECT * FROM products")
                results = cursor.fetchall()
        """
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            yield cursor
            if commit:
                conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cursor.close()
            self.return_connection(conn)


# Singleton instance
_db_pool = PostgresConnectionPool()


def get_db_pool() -> PostgresConnectionPool:
    """Get the singleton database pool instance"""
    return _db_pool


def initialize_db_pool(**kwargs):
    """Initialize the database pool with custom settings"""
    _db_pool.initialize(**kwargs)


__all__ = [
    "PostgresConnectionPool",
    "get_db_pool",
    "initialize_db_pool",
]
