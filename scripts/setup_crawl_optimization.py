#!/usr/bin/env python3
"""
Thiết lập Airflow Variables để tối ưu tốc độ crawl
Chạy: docker-compose exec -T airflow-scheduler python scripts/setup_crawl_optimization.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from airflow.models import Variable


def setup_optimization_variables():
    """Thiết lập các Airflow Variables để tối ưu crawl"""
    
    variables = {
        # === SELENIUM POOL OPTIMIZATION ===
        "TIKI_DETAIL_POOL_SIZE": "8",  # Tăng từ 5 → 8
        
        # === RATE LIMITING OPTIMIZATION ===
        "TIKI_DETAIL_RATE_LIMIT_DELAY": "0.7",  # Giảm từ 1.5 → 0.7 giây
        
        # === TIMEOUT OPTIMIZATION ===
        "TIKI_DETAIL_CRAWL_TIMEOUT": "120",  # Giảm từ 180 → 120 giây (2 phút)
        "TIKI_PAGE_LOAD_TIMEOUT": "35",  # Giảm từ 60 → 35 giây
        
        # === ASYNC OPTIMIZATION ===
        "TIKI_ASYNC_CONCURRENCY": "15",  # Số tasks async tối đa (tasks trong event loop)
        "TIKI_ASYNC_CONNECTOR_LIMIT": "50",  # HTTP connection pool limit
        "TIKI_ASYNC_CONNECTOR_LIMIT_PER_HOST": "10",  # Per-host limit
        
        # === BATCH OPTIMIZATION ===
        "TIKI_DETAIL_BATCH_SIZE": "15",  # Giữ nguyên (tối ưu)
        "TIKI_DETAIL_RETRY_COUNT": "2",  # Giảm retry từ 3 → 2 (nếu error rate thấp)
        
        # === CACHE OPTIMIZATION ===
        "TIKI_REDIS_CACHE_TTL": "86400",  # 24 hours (mặc định)
        "TIKI_REDIS_CACHE_DB": "1",  # Redis DB 1 cho cache
        
        # === MONITORING ===
        "TIKI_CRAWL_TARGET_SPEED": "1000",  # Target: 1000 products/hour
    }
    
    print("🔧 Thiết lập Airflow Variables để tối ưu crawl...\n")
    
    for key, value in variables.items():
        try:
            Variable.set(key, value)
            print(f"✅ {key} = {value}")
        except Exception as e:
            print(f"❌ {key}: {e}")
    
    print("\n📊 Tóm tắt thay đổi:")
    print("=" * 60)
    print("Selenium Pool Size:          5 → 8")
    print("Rate Limit Delay:            1.5s → 0.7s")
    print("Crawl Timeout:               180s → 120s")
    print("Page Load Timeout:           60s → 35s")
    print("Async Concurrency:           (new) 15 tasks")
    print("HTTP Connector Limit:        (new) 50 connections")
    print("=" * 60)
    print("\n💡 Expected Improvement:")
    print("   - Current:  ~300-500 products/hour")
    print("   - Target:   ~1000-1500 products/hour")
    print("   - Gain:     2-3x faster ⚡")
    print("\n⚠️  Hãy monitor error rate trong 1 giờ sau.")
    print("   Nếu error > 5%, hãy tăng TIKI_DETAIL_RATE_LIMIT_DELAY")


if __name__ == "__main__":
    try:
        setup_optimization_variables()
        print("\n✅ Hoàn tất! Restart Airflow DAG để áp dụng thay đổi.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        sys.exit(1)
