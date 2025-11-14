"""
Script để setup Airflow Variables cho Tiki Crawl DAG

Chạy script này để cấu hình các biến môi trường cho DAG crawl sản phẩm Tiki
"""
import os
import sys

# Thêm đường dẫn airflow vào sys.path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'airflow'))

try:
    from airflow.models import Variable
    from airflow.utils.db import provide_session
    
    @provide_session
    def setup_variables(session=None):
        """Setup Airflow Variables"""
        
        variables = {
            # Cấu hình crawl
            'TIKI_MAX_PAGES_PER_CATEGORY': '0',  # 0 = crawl tất cả trang
            'TIKI_MAX_CATEGORIES': '0',  # 0 = crawl tất cả danh mục, >0 = giới hạn số danh mục
            'TIKI_MIN_CATEGORY_LEVEL': '2',  # Level danh mục tối thiểu
            'TIKI_MAX_CATEGORY_LEVEL': '4',  # Level danh mục tối đa
            'TIKI_USE_SELENIUM': 'false',  # true/false - có dùng Selenium không
            'TIKI_CRAWL_TIMEOUT': '300',  # Timeout mỗi category (giây)
            'TIKI_RATE_LIMIT_DELAY': '1.0',  # Delay giữa các request (giây)
            
            # Cấu hình save
            'TIKI_SAVE_BATCH_SIZE': '10000',  # Số sản phẩm mỗi batch khi save
        }
        
        print("="*70)
        print("🔧 SETUP AIRFLOW VARIABLES CHO TIKI CRAWL DAG")
        print("="*70)
        
        for key, value in variables.items():
            try:
                # Kiểm tra xem variable đã tồn tại chưa
                existing = Variable.get(key, default_var=None)
                if existing is not None:
                    print(f"⚠️  Variable '{key}' đã tồn tại: {existing}")
                    print(f"   Giữ nguyên giá trị cũ. Để thay đổi, xóa và tạo lại.")
                else:
                    Variable.set(key, value)
                    print(f"✅ Đã tạo variable '{key}' = '{value}'")
            except Exception as e:
                print(f"❌ Lỗi khi tạo variable '{key}': {e}")
        
        print("="*70)
        print("✅ HOÀN THÀNH!")
        print("="*70)
        print("\n💡 Để thay đổi giá trị, dùng Airflow UI hoặc CLI:")
        print("   airflow variables set TIKI_MAX_CATEGORIES 10")
        print("\n📖 Xem tất cả variables:")
        print("   airflow variables list")
    
    if __name__ == "__main__":
        setup_variables()
        
except ImportError as e:
    print("❌ Không thể import Airflow. Đảm bảo Airflow đã được cài đặt và cấu hình.")
    print(f"   Lỗi: {e}")
    print("\n💡 Cách khác: Dùng Airflow CLI trực tiếp:")
    print("   airflow variables set TIKI_MAX_PAGES_PER_CATEGORY 0")
    print("   airflow variables set TIKI_MAX_CATEGORIES 0")
    print("   airflow variables set TIKI_MIN_CATEGORY_LEVEL 2")
    print("   airflow variables set TIKI_MAX_CATEGORY_LEVEL 4")
    print("   airflow variables set TIKI_USE_SELENIUM false")
    print("   airflow variables set TIKI_CRAWL_TIMEOUT 300")
    print("   airflow variables set TIKI_RATE_LIMIT_DELAY 1.0")
    print("   airflow variables set TIKI_SAVE_BATCH_SIZE 10000")

