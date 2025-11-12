"""
Script để setup Airflow pool cho Tiki crawler
Chạy script này để tạo pool và config cho crawler
"""
import os
import sys
from airflow.models import Pool
from airflow import settings

def setup_tiki_pool():
    """Tạo hoặc cập nhật Airflow pool cho Tiki crawler"""
    
    pool_name = os.getenv("TIKI_POOL_NAME", "tiki_crawler_pool")
    pool_slots = int(os.getenv("TIKI_POOL_SLOTS", "20"))
    pool_description = "Pool cho Tiki crawler tasks - giới hạn concurrent tasks"
    
    session = settings.Session()
    
    # Kiểm tra xem pool đã tồn tại chưa
    existing_pool = session.query(Pool).filter(Pool.pool == pool_name).first()
    
    if existing_pool:
        print(f"Pool '{pool_name}' đã tồn tại, đang cập nhật...")
        existing_pool.slots = pool_slots
        existing_pool.description = pool_description
        session.commit()
        print(f"✓ Đã cập nhật pool '{pool_name}' với {pool_slots} slots")
    else:
        print(f"Đang tạo pool mới '{pool_name}'...")
        new_pool = Pool(
            pool=pool_name,
            slots=pool_slots,
            description=pool_description
        )
        session.add(new_pool)
        session.commit()
        print(f"✓ Đã tạo pool '{pool_name}' với {pool_slots} slots")
    
    session.close()

if __name__ == "__main__":
    # Chạy trong Airflow environment
    setup_tiki_pool()

