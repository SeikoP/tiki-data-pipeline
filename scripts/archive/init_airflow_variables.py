#!/usr/bin/env python3
"""
Khởi tạo các Airflow Variables cần thiết với giá trị mặc định
Chạy: docker-compose exec airflow-scheduler python scripts/init_airflow_variables.py
Hoặc: docker exec -it <airflow-scheduler-container> python /opt/airflow/scripts/init_airflow_variables.py
"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from airflow.models import Variable
except ImportError:
    print("❌ Không thể import Airflow. Đảm bảo đang chạy trong Airflow container.")
    sys.exit(1)


def init_required_variables():
    """Khởi tạo các Airflow Variables bắt buộc với giá trị mặc định"""
    
    # Các variables bắt buộc với giá trị mặc định
    required_variables = {
        # === DAG CONFIGURATION ===
        "TIKI_DAG_SCHEDULE_MODE": "manual",  # 'manual' hoặc 'scheduled'
        
        # === CIRCUIT BREAKER ===
        "TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD": "5",
        "TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT": "60",
        
        # === GRACEFUL DEGRADATION ===
        "TIKI_DEGRADATION_FAILURE_THRESHOLD": "3",
        "TIKI_DEGRADATION_RECOVERY_THRESHOLD": "5",
        
        # === REDIS CONFIGURATION ===
        "REDIS_URL": "redis://redis:6379/3",
    }
    
    print("🔧 Khởi tạo Airflow Variables bắt buộc...\n")
    
    created = 0
    skipped = 0
    errors = 0
    
    for key, default_value in required_variables.items():
        try:
            # Kiểm tra xem variable đã tồn tại chưa
            try:
                existing = Variable.get(key)
                print(f"✓ {key} = {existing} (đã tồn tại, bỏ qua)")
                skipped += 1
            except Exception:
                # Variable chưa tồn tại, tạo mới
                Variable.set(key, default_value)
                print(f"✅ {key} = {default_value} (mới tạo)")
                created += 1
        except Exception as e:
            print(f"❌ {key}: {e}")
            errors += 1
    
    print("\n📊 Kết quả:")
    print("=" * 60)
    print(f"✅ Đã tạo mới: {created} variables")
    print(f"✓ Đã tồn tại: {skipped} variables")
    if errors > 0:
        print(f"❌ Lỗi: {errors} variables")
    print("=" * 60)
    
    if created > 0:
        print("\n💡 Các variables đã được khởi tạo. DAG sẽ không còn báo lỗi 'Variable not found'.")
    else:
        print("\n💡 Tất cả variables đã tồn tại. Không cần khởi tạo.")


if __name__ == "__main__":
    try:
        init_required_variables()
        print("\n✅ Hoàn tất!")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Lỗi: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

