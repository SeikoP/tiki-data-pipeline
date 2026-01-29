
import os
import sys

# Thêm src vào path
project_root = "/home/bungmoto/projects/tiki-data-pipeline"
src_path = os.path.join(project_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

try:
    from pipelines.crawl.storage.postgres_storage import PostgresStorage
    
    storage = PostgresStorage(
        host="localhost",
        user="bungmoto",
        password="0946932602a",
        database="tiki"
    )
    
    with storage.get_connection() as conn:
        with conn.cursor() as cur:
            # 1. Lấy tất cả các bảng (trừ system tables)
            cur.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_type = 'BASE TABLE'
            """)
            tables = [row[0] for row in cur.fetchall()]
            print(f"Tables found: {', '.join(tables)}\n")
            
            for table in tables:
                print(f"=== Table: {table} ===")
                # Lấy tổng số dòng
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                total_rows = cur.fetchone()[0]
                print(f"Total rows: {total_rows}")
                
                # Lấy chi tiết từng cột và số lượng NULL
                cur.execute(f"""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name = '{table}'
                    ORDER BY ordinal_position
                """)
                columns = cur.fetchall()
                
                print(f"{'Column':<25} | {'Type':<15} | {'NULL Count':<10} | {'% NULL'}")
                print("-" * 65)
                
                for col_name, data_type, is_nullable in columns:
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {col_name} IS NULL")
                    null_count = cur.fetchone()[0]
                    null_percent = (null_count / total_rows * 100) if total_rows > 0 else 0
                    print(f"{col_name:<25} | {data_type:<15} | {null_count:<10} | {null_percent:.1f}%")
                print("\n")
                
except Exception as e:
    print(f"Lỗi: {e}")
