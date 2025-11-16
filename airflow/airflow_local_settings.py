"""
Airflow Local Settings
Cấu hình để force TCP/IP connection cho PostgreSQL thay vì Unix socket

File này được Airflow tự động load khi khởi động.
connect_args sẽ được sử dụng bởi SQLAlchemy để force TCP/IP connection.
"""

# Force TCP/IP connection cho PostgreSQL
# Điều này đảm bảo psycopg2 không cố dùng Unix socket
# Bằng cách chỉ định rõ host và port trong connect_args
import os
import sys

# CRITICAL: Thêm /opt/airflow/dags vào sys.path để absolute imports hoạt động
# Airflow có thể không tự động thêm dags folder vào sys.path
_dags_dir = "/opt/airflow/dags"
if _dags_dir not in sys.path:
    sys.path.insert(0, _dags_dir)

connect_args = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),  # Hostname trong Docker network
    "port": int(os.getenv("POSTGRES_PORT", "5432")),  # Port PostgreSQL
    "connect_timeout": 10,  # Timeout khi connect
    # Force TCP/IP connection bằng cách chỉ định rõ host và port
    # Điều này ngăn psycopg2 sử dụng Unix socket
}

# Có thể thêm các connect args khác nếu cần
# connect_args["keepalives_idle"] = 600
# connect_args["keepalives_interval"] = 10
# connect_args["keepalives_count"] = 5

