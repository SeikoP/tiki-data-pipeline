#!/usr/bin/env python3
"""
Script test kết nối PostgreSQL từ Airflow container
"""

import os
import sys

try:
    import psycopg2
except ImportError:
    print("❌ psycopg2 chưa được cài đặt")
    sys.exit(1)

# Lấy thông tin từ environment
host = os.getenv("POSTGRES_HOST", "postgres")
port = int(os.getenv("POSTGRES_PORT", "5432"))
user = os.getenv("POSTGRES_USER", "airflow_user")
password = os.getenv("POSTGRES_PASSWORD", "")
database = os.getenv("POSTGRES_DB", "airflow")

print("Testing PostgreSQL connection...")
print(f"Host: {host}, Port: {port}, User: {user}, Database: {database}")

# Test 1: Connection với host và port rõ ràng (TCP/IP)
print("\n1. Testing TCP/IP connection (host + port)...")
try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        connect_timeout=10,
    )
    print("✅ TCP/IP connection thành công!")
    conn.close()
except Exception as e:
    print(f"❌ TCP/IP connection failed: {e}")

# Test 2: Connection string format
print("\n2. Testing connection string format...")
try:
    conn_str = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    conn = psycopg2.connect(conn_str, connect_timeout=10)
    print("✅ Connection string format thành công!")
    conn.close()
except Exception as e:
    print(f"❌ Connection string format failed: {e}")

# Test 3: Connection với DSN
print("\n3. Testing DSN format...")
try:
    dsn = f"host={host} port={port} dbname={database} user={user} password={password} connect_timeout=10"
    conn = psycopg2.connect(dsn)
    print("✅ DSN format thành công!")
    conn.close()
except Exception as e:
    print(f"❌ DSN format failed: {e}")

print("\n✅ Hoàn tất test!")
