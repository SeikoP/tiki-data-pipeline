"""
Configuration for Tiki crawl products DAG
"""
import os
from datetime import datetime, timedelta
from pathlib import Path

from .utils import Variable

# Cấu hình mặc định
DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,  # Retry 3 lần
    "retry_delay": timedelta(minutes=2),  # Delay 2 phút giữa các retry
    "retry_exponential_backoff": True,  # Exponential backoff
    "max_retry_delay": timedelta(minutes=10),
}

# Cấu hình DAG - Có thể chuyển đổi giữa tự động và thủ công qua Variable
# Đọc schedule mode từ Airflow Variable (mặc định: 'manual' để test)
# Có thể set Variable 'TIKI_DAG_SCHEDULE_MODE' = 'scheduled' để chạy tự động
try:
    schedule_mode = Variable.get("TIKI_DAG_SCHEDULE_MODE", default_var="manual")
except Exception:
    schedule_mode = "manual"  # Mặc định là manual để test

# Xác định schedule dựa trên mode
if schedule_mode == "scheduled":
    dag_schedule = timedelta(days=1)  # Chạy tự động hàng ngày
    dag_description = (
        "Crawl sản phẩm Tiki với Dynamic Task Mapping và tối ưu hóa (Tự động chạy hàng ngày)"
    )
    dag_tags = ["tiki", "crawl", "products", "data-pipeline", "scheduled"]
else:
    dag_schedule = None  # Chỉ chạy khi trigger thủ công
    dag_description = (
        "Crawl sản phẩm Tiki với Dynamic Task Mapping và tối ưu hóa (Chạy thủ công - Test mode)"
    )
    dag_tags = ["tiki", "crawl", "products", "data-pipeline", "manual"]

# Cấu hình DAG schedule
dag_schedule_config = dag_schedule

# Documentation đơn giản cho DAG
dag_doc_md = "Crawl sản phẩm từ Tiki.vn với Dynamic Task Mapping và Selenium"

DAG_CONFIG = {
    "dag_id": "tiki_crawl_products",
    "description": dag_description,
    "doc_md": dag_doc_md,
    "default_args": DEFAULT_ARGS,
    "schedule": dag_schedule_config,
    "start_date": datetime(2025, 11, 1),  # Ngày cố định trong quá khứ
    "catchup": False,  # Không chạy lại các task đã bỏ lỡ
    "tags": dag_tags,
    "max_active_runs": 1,  # Chỉ chạy 1 DAG instance tại một thời điểm
    "max_active_tasks": 10,  # Giảm xuống 10 tasks song song để tránh quá tải khi tạo Selenium driver
}

# Thư mục dữ liệu
# Trong Docker, data được mount vào /opt/airflow/data
# Thử nhiều đường dẫn
# Lấy đường dẫn từ DAG file (sẽ được set từ file DAG chính)
_dag_file_dir = None


def set_dag_file_dir(dag_file_dir):
    """Set đường dẫn thư mục chứa DAG file"""
    global _dag_file_dir
    _dag_file_dir = dag_file_dir


def get_dag_file_dir():
    """Lấy đường dẫn thư mục chứa DAG file"""
    global _dag_file_dir
    if _dag_file_dir:
        return _dag_file_dir
    # Fallback: tính từ config.py (lên 1 cấp đến dags/)
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


possible_data_dirs = [
    Path("/opt/airflow/data"),  # Docker mount
    Path(get_dag_file_dir()).parent.parent.parent / "data",  # Local development
    Path(os.getcwd()) / "data",  # Current working directory
]

DATA_DIR = None
for data_dir in possible_data_dirs:
    if data_dir.exists():
        DATA_DIR = data_dir
        break

if not DATA_DIR:
    # Fallback: dùng đường dẫn tương đối
    DATA_DIR = Path(get_dag_file_dir()).parent.parent.parent / "data"

CATEGORIES_FILE = DATA_DIR / "raw" / "categories_recursive_optimized.json"
CATEGORIES_TREE_FILE = DATA_DIR / "raw" / "categories_tree.json"
OUTPUT_DIR = DATA_DIR / "raw" / "products"
CACHE_DIR = OUTPUT_DIR / "cache"
DETAIL_CACHE_DIR = OUTPUT_DIR / "detail" / "cache"
OUTPUT_FILE = OUTPUT_DIR / "products.json"
OUTPUT_FILE_WITH_DETAIL = OUTPUT_DIR / "products_with_detail.json"
# Progress tracking cho multi-day crawling
PROGRESS_FILE = OUTPUT_DIR / "crawl_progress.json"

# Tạo thư mục nếu chưa có
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DETAIL_CACHE_DIR.mkdir(parents=True, exist_ok=True)

