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
        # === DAG SCHEDULE MODE ===
        "TIKI_DAG_SCHEDULE_MODE": "manual",  # 'manual' hoặc 'scheduled'
        # === CIRCUIT BREAKER ===
        "TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD": "5",  # Số lỗi tối đa trước khi mở circuit
        "TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT": "60",  # Thời gian chờ trước khi thử lại (giây)
        # === GRACEFUL DEGRADATION ===
        "TIKI_DEGRADATION_FAILURE_THRESHOLD": "3",  # Số lỗi để bắt đầu degradation
        "TIKI_DEGRADATION_RECOVERY_THRESHOLD": "5",  # Số success để recovery
        # === REDIS CONFIGURATION ===
        "REDIS_URL": "redis://redis:6379/3",  # Redis URL cho DLQ và các services khác
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

    created = 0
    updated = 0
    errors = 0

    for key, value in variables.items():
        try:
            # Kiểm tra xem variable đã tồn tại chưa
            try:
                existing = Variable.get(key)
                Variable.set(key, value)
                if existing != value:
                    print(f"🔄 {key} = {value} (đã cập nhật từ {existing})")
                    updated += 1
                else:
                    print(f"✓ {key} = {value} (không đổi)")
            except Exception:
                # Variable chưa tồn tại, tạo mới
                Variable.set(key, value)
                print(f"✅ {key} = {value} (mới tạo)")
                created += 1
        except Exception as e:
            print(f"❌ {key}: {e}")
            errors += 1

    print("\n📊 Tóm tắt:")
    print("=" * 60)
    print(f"✅ Đã tạo mới: {created} variables")
    print(f"🔄 Đã cập nhật: {updated} variables")
    if errors > 0:
        print(f"❌ Lỗi: {errors} variables")
    print("=" * 60)
    print("\n📋 Các Variables quan trọng:")
    print("   - TIKI_DAG_SCHEDULE_MODE: 'manual' (test) hoặc 'scheduled' (production)")
    print("   - TIKI_CIRCUIT_BREAKER_*: Circuit breaker configuration")
    print("   - TIKI_DEGRADATION_*: Graceful degradation configuration")
    print("   - REDIS_URL: Redis connection cho DLQ")
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
