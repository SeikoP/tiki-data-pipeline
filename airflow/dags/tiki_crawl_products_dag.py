"""
DAG Airflow để crawl sản phẩm Tiki với tối ưu hóa cho dữ liệu lớn

Tính năng:
- Dynamic Task Mapping: crawl song song nhiều danh mục
- Chia nhỏ tasks: mỗi task một chức năng riêng
- XCom: chia sẻ dữ liệu giữa các tasks
- Retry: tự động retry khi lỗi
- Timeout: giới hạn thời gian thực thi
- Logging: ghi log rõ ràng cho từng task
- Error handling: xử lý lỗi và tiếp tục với danh mục khác
- Atomic writes: ghi file an toàn, tránh corrupt
- TaskGroup: nhóm các tasks liên quan
- Tối ưu: batch processing, rate limiting, caching
"""
import os
import sys
import json
import time
import hashlib
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup
from airflow.models import Variable
from airflow.configuration import conf
from airflow.utils.session import provide_session

# Thêm đường dẫn src vào sys.path
# Lấy đường dẫn tuyệt đối của DAG file
dag_file_dir = os.path.dirname(os.path.abspath(__file__))

# Thử nhiều đường dẫn có thể
# Trong Docker, src được mount vào /opt/airflow/src
possible_paths = [
    # Từ /opt/airflow (Docker default - ưu tiên)
    '/opt/airflow/src/pipelines/crawl',
    # Từ airflow/dags/ lên 2 cấp đến root (local development)
    os.path.abspath(os.path.join(dag_file_dir, '..', '..', 'src', 'pipelines', 'crawl')),
    # Từ airflow/dags/ lên 1 cấp (nếu airflow/ là root)
    os.path.abspath(os.path.join(dag_file_dir, '..', 'src', 'pipelines', 'crawl')),
    # Từ workspace root (nếu mount vào /workspace)
    '/workspace/src/pipelines/crawl',
    # Từ current working directory
    os.path.join(os.getcwd(), 'src', 'pipelines', 'crawl'),
]

# Tìm đường dẫn hợp lệ
crawl_module_path = None
crawl_products_path = None

for path in possible_paths:
    test_path = os.path.join(path, 'crawl_products.py')
    if os.path.exists(test_path):
        crawl_module_path = path
        crawl_products_path = test_path
        break

if not crawl_module_path:
    # Nếu không tìm thấy, thử đường dẫn tương đối từ DAG file
    relative_path = os.path.abspath(os.path.join(dag_file_dir, '..', '..', 'src', 'pipelines', 'crawl'))
    test_path = os.path.join(relative_path, 'crawl_products.py')
    if os.path.exists(test_path):
        crawl_module_path = relative_path
        crawl_products_path = test_path

# Import module
if crawl_products_path and os.path.exists(crawl_products_path):
    # Sử dụng importlib để import trực tiếp từ file (cách đáng tin cậy nhất)
    import importlib.util
    spec = importlib.util.spec_from_file_location("crawl_products", crawl_products_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Không thể load spec từ {crawl_products_path}")
    crawl_products_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(crawl_products_module)
    
    # Extract các functions cần thiết
    crawl_category_products = crawl_products_module.crawl_category_products
    get_page_with_requests = crawl_products_module.get_page_with_requests
    parse_products_from_html = crawl_products_module.parse_products_from_html
    get_total_pages = crawl_products_module.get_total_pages
else:
    # Fallback: thử import thông thường nếu đã thêm vào sys.path
    if crawl_module_path and crawl_module_path not in sys.path:
        sys.path.insert(0, crawl_module_path)
    
    try:
        from crawl_products import (
            crawl_category_products,
            get_page_with_requests,
            parse_products_from_html,
            get_total_pages
        )
    except ImportError as e:
        # Debug: kiểm tra xem thư mục có tồn tại không
        debug_info = {
            'dag_file_dir': dag_file_dir,
            'cwd': os.getcwd(),
            'possible_paths': possible_paths,
            'crawl_module_path': crawl_module_path,
            'crawl_products_path': crawl_products_path,
            'sys_path': sys.path[:5]  # Chỉ lấy 5 đầu tiên
        }
        
        # Kiểm tra xem /opt/airflow/src có tồn tại không
        if os.path.exists('/opt/airflow/src'):
            try:
                debug_info['opt_airflow_src_contents'] = os.listdir('/opt/airflow/src')
            except:
                pass
        
        raise ImportError(
            f"Không tìm thấy module crawl_products.\n"
            f"Debug info: {debug_info}\n"
            f"Lỗi gốc: {e}"
        )

# Cấu hình mặc định
DEFAULT_ARGS = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,  # Retry 3 lần
    'retry_delay': timedelta(minutes=2),  # Delay 2 phút giữa các retry
    'retry_exponential_backoff': True,  # Exponential backoff
    'max_retry_delay': timedelta(minutes=10),
}

# Cấu hình DAG
DAG_CONFIG = {
    'dag_id': 'tiki_crawl_products',
    'description': 'Crawl sản phẩm Tiki với Dynamic Task Mapping và tối ưu hóa',
    'default_args': DEFAULT_ARGS,
    'schedule': timedelta(days=1),  # Chạy hàng ngày
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
    'tags': ['tiki', 'crawl', 'products', 'data-pipeline'],
    'max_active_runs': 1,  # Chỉ chạy 1 DAG instance tại một thời điểm
    'max_active_tasks': 20,  # Tối đa 20 tasks song song
}

# Thư mục dữ liệu
# Trong Docker, data được mount vào /opt/airflow/data
# Thử nhiều đường dẫn
possible_data_dirs = [
    Path('/opt/airflow/data'),  # Docker mount
    Path(__file__).parent.parent.parent / 'data',  # Local development
    Path(os.getcwd()) / 'data',  # Current working directory
]

DATA_DIR = None
for data_dir in possible_data_dirs:
    if data_dir.exists():
        DATA_DIR = data_dir
        break

if not DATA_DIR:
    # Fallback: dùng đường dẫn tương đối
    DATA_DIR = Path(__file__).parent.parent.parent / 'data'

CATEGORIES_FILE = DATA_DIR / 'raw' / 'categories_recursive_optimized.json'
OUTPUT_DIR = DATA_DIR / 'raw' / 'products'
CACHE_DIR = OUTPUT_DIR / 'cache'
OUTPUT_FILE = OUTPUT_DIR / 'products.json'

# Tạo thư mục nếu chưa có
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Thread-safe lock cho atomic writes
write_lock = Lock()


def get_logger(context):
    """Lấy logger từ context (Airflow 3.x compatible)"""
    try:
        # Airflow 3.x: sử dụng logging module
        import logging
        ti = context.get('task_instance')
        if ti:
            # Tạo logger với task_id và dag_id
            logger_name = f"airflow.task.{ti.dag_id}.{ti.task_id}"
            return logging.getLogger(logger_name)
        else:
            # Fallback: dùng root logger
            return logging.getLogger("airflow.task")
    except Exception:
        # Fallback: dùng root logger
        import logging
        return logging.getLogger("airflow.task")


def load_categories(**context) -> List[Dict[str, Any]]:
    """
    Task 1: Load danh sách danh mục từ file
    
    Returns:
        List[Dict]: Danh sách danh mục
    """
    logger = get_logger(context)
    logger.info("="*70)
    logger.info("📖 TASK: Load Categories")
    logger.info("="*70)
    
    try:
        categories_file = str(CATEGORIES_FILE)
        logger.info(f"Đang đọc file: {categories_file}")
        
        if not os.path.exists(categories_file):
            raise FileNotFoundError(f"Không tìm thấy file: {categories_file}")
        
        with open(categories_file, 'r', encoding='utf-8') as f:
            categories = json.load(f)
        
        logger.info(f"✅ Đã load {len(categories)} danh mục")
        
        # Lọc danh mục nếu cần (ví dụ: chỉ lấy level 2-4)
        # Có thể cấu hình qua Airflow Variable
        try:
            min_level = int(Variable.get('TIKI_MIN_CATEGORY_LEVEL', default_var='2'))
            max_level = int(Variable.get('TIKI_MAX_CATEGORY_LEVEL', default_var='4'))
            categories = [
                cat for cat in categories 
                if min_level <= cat.get('level', 0) <= max_level
            ]
            logger.info(f"✓ Sau khi lọc level {min_level}-{max_level}: {len(categories)} danh mục")
        except Exception as e:
            logger.warning(f"Không thể lọc theo level: {e}")
        
        # Giới hạn số danh mục nếu cần (để test)
        try:
            max_categories = int(Variable.get('TIKI_MAX_CATEGORIES', default_var='0'))
            if max_categories > 0:
                categories = categories[:max_categories]
                logger.info(f"✓ Giới hạn: {max_categories} danh mục")
        except:
            pass
        
        # Push categories lên XCom để các task khác dùng
        return categories
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi load categories: {e}", exc_info=True)
        raise


def crawl_single_category(category: Dict[str, Any] = None, **context) -> Dict[str, Any]:
    """
    Task 2: Crawl sản phẩm từ một danh mục (Dynamic Task Mapping)
    
    Tối ưu hóa:
    - Rate limiting: delay giữa các request
    - Caching: sử dụng cache để tránh crawl lại
    - Error handling: tiếp tục với danh mục khác khi lỗi
    - Timeout: giới hạn thời gian crawl
    
    Args:
        category: Thông tin danh mục (từ expand_kwargs)
        context: Airflow context
        
    Returns:
        Dict: Kết quả crawl với products và metadata
    """
    logger = get_logger(context)
    
    # Lấy category từ keyword argument hoặc từ op_kwargs trong context
    # Khi sử dụng expand với op_kwargs, category sẽ được truyền qua op_kwargs
    if not category:
        # Thử lấy từ ti.op_kwargs (cách chính xác nhất)
        ti = context.get('ti')
        if ti:
            # op_kwargs được truyền vào function thông qua ti
            op_kwargs = getattr(ti, 'op_kwargs', {})
            if op_kwargs:
                category = op_kwargs.get('category')
        
        # Fallback: thử lấy từ context trực tiếp
        if not category:
            category = context.get('category') or context.get('op_kwargs', {}).get('category')
    
    if not category:
        # Debug: log context để tìm lỗi
        logger.error(f"Không tìm thấy category. Context keys: {list(context.keys())}")
        ti = context.get('ti')
        if ti:
            logger.error(f"ti.op_kwargs: {getattr(ti, 'op_kwargs', 'N/A')}")
        raise ValueError("Không tìm thấy category. Kiểm tra expand với op_kwargs.")
    
    category_url = category.get('url', '')
    category_name = category.get('name', 'Unknown')
    category_id = category.get('id', '')
    
    logger.info("="*70)
    logger.info(f"🛍️  TASK: Crawl Category - {category_name}")
    logger.info(f"🔗 URL: {category_url}")
    logger.info("="*70)
    
    result = {
        'category_id': category_id,
        'category_name': category_name,
        'category_url': category_url,
        'products': [],
        'status': 'failed',
        'error': None,
        'crawled_at': datetime.now().isoformat(),
        'pages_crawled': 0,
        'products_count': 0
    }
    
    try:
        # Lấy cấu hình từ Airflow Variables
        max_pages = int(Variable.get('TIKI_MAX_PAGES_PER_CATEGORY', default_var='20'))  # Mặc định 20 trang để tránh timeout
        use_selenium = Variable.get('TIKI_USE_SELENIUM', default_var='false').lower() == 'true'
        timeout = int(Variable.get('TIKI_CRAWL_TIMEOUT', default_var='300'))  # 5 phút mặc định
        rate_limit_delay = float(Variable.get('TIKI_RATE_LIMIT_DELAY', default_var='1.0'))  # Delay 1s giữa các request
        
        # Rate limiting: delay trước khi crawl
        if rate_limit_delay > 0:
            time.sleep(rate_limit_delay)
        
        # Crawl với timeout
        start_time = time.time()
        
        products = crawl_category_products(
            category_url,
            max_pages=max_pages if max_pages > 0 else None,
            use_selenium=use_selenium,
            cache_dir=str(CACHE_DIR)
        )
        
        elapsed = time.time() - start_time
        
        if elapsed > timeout:
            raise TimeoutError(f"Crawl vượt quá timeout {timeout}s")
        
        result['products'] = products
        result['status'] = 'success'
        result['products_count'] = len(products)
        result['elapsed_time'] = elapsed
        
        logger.info(f"✅ Crawl thành công: {len(products)} sản phẩm trong {elapsed:.1f}s")
        
    except TimeoutError as e:
        result['error'] = str(e)
        result['status'] = 'timeout'
        logger.error(f"⏱️  Timeout: {e}")
        # Không raise để tiếp tục với danh mục khác
        
    except Exception as e:
        result['error'] = str(e)
        result['status'] = 'failed'
        logger.error(f"❌ Lỗi khi crawl category {category_name}: {e}", exc_info=True)
        # Không raise để tiếp tục với danh mục khác
    
    return result


def merge_products(**context) -> Dict[str, Any]:
    """
    Task 3: Merge sản phẩm từ tất cả các danh mục
    
    Returns:
        Dict: Tổng hợp sản phẩm và thống kê
    """
    logger = get_logger(context)
    logger.info("="*70)
    logger.info("🔄 TASK: Merge Products")
    logger.info("="*70)
    
    try:
        from airflow.models import TaskInstance
        from airflow.models.dagrun import DagRun
        
        ti = context['ti']
        dag_run = context['dag_run']
        
        # Lấy categories từ task load_categories (trong TaskGroup load_and_prepare)
        # Thử nhiều cách để lấy categories
        categories = None
        
        # Cách 1: Lấy từ task_id với TaskGroup prefix
        try:
            categories = ti.xcom_pull(task_ids='load_and_prepare.load_categories')
            logger.info(f"Lấy categories từ 'load_and_prepare.load_categories': {len(categories) if categories else 0} items")
        except Exception as e:
            logger.warning(f"Không lấy được từ 'load_and_prepare.load_categories': {e}")
        
        # Cách 2: Thử không có prefix
        if not categories:
            try:
                categories = ti.xcom_pull(task_ids='load_categories')
                logger.info(f"Lấy categories từ 'load_categories': {len(categories) if categories else 0} items")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'load_categories': {e}")
        
        if not categories:
            raise ValueError("Không tìm thấy categories từ XCom")
        
        logger.info(f"Đang merge kết quả từ {len(categories)} danh mục...")
        
        # Lấy kết quả từ các task crawl (Dynamic Task Mapping)
        # Với Dynamic Task Mapping, cần lấy từ task_id với map_index
        all_products = []
        stats = {
            'total_categories': len(categories),
            'success_categories': 0,
            'failed_categories': 0,
            'timeout_categories': 0,
            'total_products': 0,
            'unique_products': 0
        }
        
        # Lấy kết quả từ các task crawl (Dynamic Task Mapping)
        # Với Dynamic Task Mapping trong Airflow 2.x, cần lấy từ task_id với map_index
        task_id = 'crawl_categories.crawl_category'
        
        # Lấy từ XCom - thử nhiều cách
        try:
            # Cách 1: Lấy tất cả kết quả từ XCom (Airflow 2.x có thể trả về list)
            all_results = ti.xcom_pull(
                task_ids=task_id,
                key='return_value'
            )
            
            # Xử lý kết quả
            if isinstance(all_results, list):
                # Nếu là list, xử lý từng phần tử
                for result in all_results:
                    if result and isinstance(result, dict):
                        if result.get('status') == 'success':
                            stats['success_categories'] += 1
                            products = result.get('products', [])
                            all_products.extend(products)
                            stats['total_products'] += len(products)
                        elif result.get('status') == 'timeout':
                            stats['timeout_categories'] += 1
                            logger.warning(f"⏱️  Category {result.get('category_name')} timeout")
                        else:
                            stats['failed_categories'] += 1
                            logger.warning(f"❌ Category {result.get('category_name')} failed: {result.get('error')}")
            elif isinstance(all_results, dict):
                # Nếu là dict, có thể key là map_index hoặc category_id
                for result in all_results.values():
                    if result and isinstance(result, dict):
                        if result.get('status') == 'success':
                            stats['success_categories'] += 1
                            products = result.get('products', [])
                            all_products.extend(products)
                            stats['total_products'] += len(products)
                        elif result.get('status') == 'timeout':
                            stats['timeout_categories'] += 1
                            logger.warning(f"⏱️  Category {result.get('category_name')} timeout")
                        else:
                            stats['failed_categories'] += 1
                            logger.warning(f"❌ Category {result.get('category_name')} failed: {result.get('error')}")
            elif all_results and isinstance(all_results, dict):
                # Nếu chỉ có 1 kết quả (dict)
                if all_results.get('status') == 'success':
                    stats['success_categories'] += 1
                    products = all_results.get('products', [])
                    all_products.extend(products)
                    stats['total_products'] += len(products)
                elif all_results.get('status') == 'timeout':
                    stats['timeout_categories'] += 1
                    logger.warning(f"⏱️  Category {all_results.get('category_name')} timeout")
                else:
                    stats['failed_categories'] += 1
                    logger.warning(f"❌ Category {all_results.get('category_name')} failed: {all_results.get('error')}")
            
            # Nếu không lấy được, thử lấy từng map_index
            if not all_results or (isinstance(all_results, (list, dict)) and len(all_results) == 0):
                logger.info("Thử lấy từng map_index...")
                for map_index in range(len(categories)):
                    try:
                        result = ti.xcom_pull(
                            task_ids=task_id,
                            key='return_value',
                            map_indexes=[map_index]
                        )
                        
                        if result and isinstance(result, dict):
                            if result.get('status') == 'success':
                                stats['success_categories'] += 1
                                products = result.get('products', [])
                                all_products.extend(products)
                                stats['total_products'] += len(products)
                            elif result.get('status') == 'timeout':
                                stats['timeout_categories'] += 1
                                logger.warning(f"⏱️  Category {result.get('category_name')} timeout")
                            else:
                                stats['failed_categories'] += 1
                                logger.warning(f"❌ Category {result.get('category_name')} failed: {result.get('error')}")
                    except Exception as e:
                        stats['failed_categories'] += 1
                        logger.warning(f"Không thể lấy kết quả từ map_index {map_index}: {e}")
        
        except Exception as e:
            logger.error(f"Không thể lấy kết quả từ XCom: {e}", exc_info=True)
            # Nếu không lấy được, đánh dấu tất cả là failed
            stats['failed_categories'] = len(categories)
        
        # Loại bỏ trùng lặp theo product_id
        seen_ids = set()
        unique_products = []
        for product in all_products:
            product_id = product.get('product_id')
            if product_id and product_id not in seen_ids:
                seen_ids.add(product_id)
                unique_products.append(product)
        
        stats['unique_products'] = len(unique_products)
        
        logger.info("="*70)
        logger.info("📊 THỐNG KÊ")
        logger.info("="*70)
        logger.info(f"📁 Tổng danh mục: {stats['total_categories']}")
        logger.info(f"✅ Thành công: {stats['success_categories']}")
        logger.info(f"❌ Thất bại: {stats['failed_categories']}")
        logger.info(f"⏱️  Timeout: {stats['timeout_categories']}")
        logger.info(f"📦 Tổng sản phẩm (trước dedup): {stats['total_products']}")
        logger.info(f"📦 Sản phẩm unique: {stats['unique_products']}")
        logger.info("="*70)
        
        result = {
            'products': unique_products,
            'stats': stats,
            'merged_at': datetime.now().isoformat()
        }
        
        return result
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi merge products: {e}", exc_info=True)
        raise


def atomic_write_file(filepath: str, data: Any, **context):
    """
    Ghi file an toàn (atomic write) để tránh corrupt
    
    Sử dụng temporary file và rename để đảm bảo atomicity
    """
    logger = get_logger(context)
    
    filepath = Path(filepath)
    temp_file = filepath.with_suffix('.tmp')
    
    try:
        # Ghi vào temporary file
        with open(temp_file, 'w', encoding='utf-8') as f:
            if isinstance(data, dict):
                json.dump(data, f, ensure_ascii=False, indent=2)
            else:
                f.write(str(data))
        
        # Atomic rename (trên Unix) hoặc move (trên Windows)
        if os.name == 'nt':  # Windows
            # Trên Windows, cần xóa file cũ trước
            if filepath.exists():
                filepath.unlink()
            shutil.move(str(temp_file), str(filepath))
        else:  # Unix/Linux
            os.rename(str(temp_file), str(filepath))
        
        logger.info(f"✅ Đã ghi file atomic: {filepath}")
        
    except Exception as e:
        # Xóa temp file nếu có lỗi
        if temp_file.exists():
            temp_file.unlink()
        logger.error(f"❌ Lỗi khi ghi file: {e}", exc_info=True)
        raise


def save_products(**context) -> str:
    """
    Task 4: Lưu sản phẩm vào file (atomic write)
    
    Tối ưu hóa cho dữ liệu lớn:
    - Batch processing: chia nhỏ và lưu từng batch
    - Atomic write: tránh corrupt file
    - Compression: có thể nén file nếu cần
    
    Returns:
        str: Đường dẫn file đã lưu
    """
    logger = get_logger(context)
    logger.info("="*70)
    logger.info("💾 TASK: Save Products")
    logger.info("="*70)
    
    try:
        # Lấy kết quả từ task merge_products (trong TaskGroup process_and_save)
        ti = context['ti']
        merge_result = None
        
        # Cách 1: Lấy từ task_id với TaskGroup prefix
        try:
            merge_result = ti.xcom_pull(task_ids='process_and_save.merge_products')
            logger.info(f"Lấy merge_result từ 'process_and_save.merge_products'")
        except Exception as e:
            logger.warning(f"Không lấy được từ 'process_and_save.merge_products': {e}")
        
        # Cách 2: Thử không có prefix
        if not merge_result:
            try:
                merge_result = ti.xcom_pull(task_ids='merge_products')
                logger.info(f"Lấy merge_result từ 'merge_products'")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'merge_products': {e}")
        
        if not merge_result:
            raise ValueError("Không tìm thấy kết quả merge từ XCom")
        
        products = merge_result.get('products', [])
        stats = merge_result.get('stats', {})
        
        logger.info(f"Đang lưu {len(products)} sản phẩm...")
        
        # Batch processing cho dữ liệu lớn
        batch_size = int(Variable.get('TIKI_SAVE_BATCH_SIZE', default_var='10000'))
        
        if len(products) > batch_size:
            logger.info(f"Chia nhỏ thành batches (mỗi batch {batch_size} sản phẩm)...")
            # Lưu từng batch vào file riêng, sau đó merge
            batch_files = []
            for i in range(0, len(products), batch_size):
                batch = products[i:i + batch_size]
                batch_file = OUTPUT_DIR / f'products_batch_{i // batch_size}.json'
                batch_data = {
                    'batch_index': i // batch_size,
                    'total_batches': (len(products) + batch_size - 1) // batch_size,
                    'products': batch
                }
                atomic_write_file(str(batch_file), batch_data, **context)
                batch_files.append(batch_file)
                logger.info(f"✓ Đã lưu batch {i // batch_size + 1}: {len(batch)} sản phẩm")
        
        # Chuẩn bị dữ liệu để lưu
        output_data = {
            'total_products': len(products),
            'stats': stats,
            'crawled_at': datetime.now().isoformat(),
            'note': 'Crawl từ Airflow DAG với Dynamic Task Mapping',
            'products': products
        }
        
        # Atomic write
        output_file = str(OUTPUT_FILE)
        atomic_write_file(output_file, output_data, **context)
        
        logger.info(f"✅ Đã lưu {len(products)} sản phẩm vào: {output_file}")
        
        return output_file
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi save products: {e}", exc_info=True)
        raise


def validate_data(**context) -> Dict[str, Any]:
    """
    Task 5: Validate dữ liệu đã crawl
    
    Returns:
        Dict: Kết quả validation
    """
    logger = get_logger(context)
    logger.info("="*70)
    logger.info("✅ TASK: Validate Data")
    logger.info("="*70)
    
    try:
        ti = context['ti']
        output_file = None
        
        # Cách 1: Lấy từ task_id với TaskGroup prefix
        try:
            output_file = ti.xcom_pull(task_ids='process_and_save.save_products')
            logger.info(f"Lấy output_file từ 'process_and_save.save_products': {output_file}")
        except Exception as e:
            logger.warning(f"Không lấy được từ 'process_and_save.save_products': {e}")
        
        # Cách 2: Thử không có prefix
        if not output_file:
            try:
                output_file = ti.xcom_pull(task_ids='save_products')
                logger.info(f"Lấy output_file từ 'save_products': {output_file}")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'save_products': {e}")
        
        if not output_file or not os.path.exists(output_file):
            raise FileNotFoundError(f"Không tìm thấy file output: {output_file}")
        
        logger.info(f"Đang validate file: {output_file}")
        
        with open(output_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        products = data.get('products', [])
        
        # Validation
        validation_result = {
            'file_exists': True,
            'total_products': len(products),
            'valid_products': 0,
            'invalid_products': 0,
            'errors': []
        }
        
        required_fields = ['product_id', 'name', 'url']
        
        for i, product in enumerate(products):
            is_valid = True
            missing_fields = []
            
            for field in required_fields:
                if not product.get(field):
                    is_valid = False
                    missing_fields.append(field)
            
            if is_valid:
                validation_result['valid_products'] += 1
            else:
                validation_result['invalid_products'] += 1
                validation_result['errors'].append({
                    'index': i,
                    'product_id': product.get('product_id'),
                    'missing_fields': missing_fields
                })
        
        logger.info("="*70)
        logger.info("📊 VALIDATION RESULTS")
        logger.info("="*70)
        logger.info(f"✅ Valid products: {validation_result['valid_products']}")
        logger.info(f"❌ Invalid products: {validation_result['invalid_products']}")
        logger.info("="*70)
        
        if validation_result['invalid_products'] > 0:
            logger.warning(f"Có {validation_result['invalid_products']} sản phẩm không hợp lệ")
            # Không fail task, chỉ warning
        
        return validation_result
        
    except Exception as e:
        logger.error(f"❌ Lỗi khi validate data: {e}", exc_info=True)
        raise


# Tạo DAG
with DAG(**DAG_CONFIG) as dag:
    
    # TaskGroup: Load và Prepare
    with TaskGroup('load_and_prepare', tooltip='Load categories và chuẩn bị') as load_group:
        task_load_categories = PythonOperator(
            task_id='load_categories',
            python_callable=load_categories,
            execution_timeout=timedelta(minutes=5),  # Timeout 5 phút
            pool='default_pool',
        )
    
    # TaskGroup: Crawl Categories (Dynamic Task Mapping)
    with TaskGroup('crawl_categories', tooltip='Crawl sản phẩm từ các danh mục') as crawl_group:
        # Sử dụng expand để Dynamic Task Mapping
        # Cần một task helper để lấy categories và tạo list op_kwargs
        def prepare_crawl_kwargs(**context):
            """Helper function để prepare op_kwargs cho Dynamic Task Mapping"""
            import logging
            logger = logging.getLogger("airflow.task")
            
            ti = context['ti']
            
            # Thử nhiều cách lấy categories từ XCom
            categories = None
            
            # Cách 1: Lấy từ task_id với TaskGroup prefix
            try:
                categories = ti.xcom_pull(task_ids='load_and_prepare.load_categories')
                logger.info(f"Lấy categories từ 'load_and_prepare.load_categories': {len(categories) if categories else 0} items")
            except Exception as e:
                logger.warning(f"Không lấy được từ 'load_and_prepare.load_categories': {e}")
            
            # Cách 2: Thử không có prefix
            if not categories:
                try:
                    categories = ti.xcom_pull(task_ids='load_categories')
                    logger.info(f"Lấy categories từ 'load_categories': {len(categories) if categories else 0} items")
                except Exception as e:
                    logger.warning(f"Không lấy được từ 'load_categories': {e}")
            
            # Cách 3: Thử lấy từ upstream task
            if not categories:
                try:
                    # Lấy từ task trong cùng DAG run
                    from airflow.models import TaskInstance
                    dag_run = context['dag_run']
                    upstream_ti = TaskInstance(
                        task=dag.get_task('load_and_prepare.load_categories'),
                        run_id=dag_run.run_id
                    )
                    categories = upstream_ti.xcom_pull(key='return_value')
                    logger.info(f"Lấy categories từ TaskInstance: {len(categories) if categories else 0} items")
                except Exception as e:
                    logger.warning(f"Không lấy được từ TaskInstance: {e}")
            
            if not categories:
                logger.error("❌ Không thể lấy categories từ XCom!")
                return []
            
            if not isinstance(categories, list):
                logger.error(f"❌ Categories không phải list: {type(categories)}")
                return []
            
            logger.info(f"✅ Đã lấy {len(categories)} categories, tạo {len(categories)} tasks cho Dynamic Task Mapping")
            
            # Trả về list các dict để expand
            return [{'category': cat} for cat in categories]
        
        task_prepare_crawl = PythonOperator(
            task_id='prepare_crawl_kwargs',
            python_callable=prepare_crawl_kwargs,
            execution_timeout=timedelta(minutes=1),
        )
        
        # Dynamic Task Mapping với expand
        # Sử dụng expand với op_kwargs để tránh lỗi với PythonOperator constructor
        task_crawl_category = PythonOperator.partial(
            task_id='crawl_category',
            python_callable=crawl_single_category,
            execution_timeout=timedelta(minutes=10),  # Timeout 10 phút mỗi category
            pool='default_pool',  # Có thể tạo pool riêng nếu cần
            retries=1,  # Retry 1 lần (tổng 2 lần thử: 1 lần đầu + 1 retry)
        ).expand(
            op_kwargs=task_prepare_crawl.output
        )
    
    # TaskGroup: Process và Save
    with TaskGroup('process_and_save', tooltip='Merge và lưu sản phẩm') as process_group:
        task_merge_products = PythonOperator(
            task_id='merge_products',
            python_callable=merge_products,
            execution_timeout=timedelta(minutes=30),  # Timeout 30 phút
            pool='default_pool',
            trigger_rule='all_done',  # QUAN TRỌNG: Chạy khi tất cả upstream tasks done (success hoặc failed)
        )
        
        task_save_products = PythonOperator(
            task_id='save_products',
            python_callable=save_products,
            execution_timeout=timedelta(minutes=10),  # Timeout 10 phút
            pool='default_pool',
        )
    
    # TaskGroup: Validate
    with TaskGroup('validate', tooltip='Validate dữ liệu') as validate_group:
        task_validate_data = PythonOperator(
            task_id='validate_data',
            python_callable=validate_data,
            execution_timeout=timedelta(minutes=5),  # Timeout 5 phút
            pool='default_pool',
        )
    
    # Định nghĩa dependencies
    task_load_categories >> task_prepare_crawl >> task_crawl_category >> task_merge_products >> task_save_products >> task_validate_data

