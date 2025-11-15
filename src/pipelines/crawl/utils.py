"""
Shared utilities cho crawl pipeline
Tối ưu: loại bỏ code duplication, cải thiện performance
"""
import sys
import os
import re
import json
import time
from typing import Optional, Union, Dict, Any
from pathlib import Path


# ============================================================================
# UTF-8 Encoding Setup (tránh lặp lại ở nhiều file)
# ============================================================================
def setup_utf8_encoding():
    """Setup UTF-8 encoding cho stdout trên Windows"""
    if sys.platform == 'win32':
        try:
            if hasattr(sys.stdout, 'buffer') and not sys.stdout.closed:
                sys.stdout.reconfigure(encoding='utf-8', errors='replace')
        except:
            try:
                import io
                if hasattr(sys.stdout, 'buffer'):
                    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            except:
                pass


# ============================================================================
# Selenium Setup (shared configuration)
# ============================================================================
def get_selenium_options(headless: bool = True) -> Optional[Any]:
    """
    Tạo Chrome options cho Selenium (shared config)
    Tối ưu hóa để khởi động nhanh hơn
    
    Returns:
        Chrome Options object hoặc None nếu Selenium không có
    """
    try:
        from selenium.webdriver.chrome.options import Options
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-extensions")
        # Tối ưu hóa để khởi động nhanh hơn
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-features=TranslateUI")
        chrome_options.add_argument("--disable-ipc-flooding-protection")
        chrome_options.add_argument("--disable-hang-monitor")
        chrome_options.add_argument("--disable-prompt-on-repost")
        chrome_options.add_argument("--disable-sync")
        chrome_options.add_argument("--disable-default-apps")
        chrome_options.add_argument("--disable-component-update")
        chrome_options.add_argument("--disable-background-networking")
        chrome_options.add_argument("--disable-breakpad")
        chrome_options.add_argument("--disable-client-side-phishing-detection")
        chrome_options.add_argument("--disable-crash-reporter")
        chrome_options.add_argument("--disable-domain-reliability")
        chrome_options.add_argument("--disable-features=AudioServiceOutOfProcess")
        chrome_options.add_argument("--disable-features=IsolateOrigins,site-per-process")
        chrome_options.add_argument("--metrics-recording-only")
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--safebrowsing-disable-auto-update")
        chrome_options.add_argument("--enable-automation")
        chrome_options.add_argument("--password-store=basic")
        chrome_options.add_argument("--use-mock-keychain")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        # Tắt images để tăng tốc (nếu không cần)
        prefs = {
            "profile.managed_default_content_settings.images": 2,  # Block images
            "profile.default_content_setting_values.notifications": 2
        }
        chrome_options.add_experimental_option("prefs", prefs)
        return chrome_options
    except ImportError:
        return None


def create_selenium_driver(headless: bool = True, timeout: int = 60) -> Optional[Any]:
    """
    Tạo Selenium WebDriver với cấu hình tối ưu và timeout
    
    Tối ưu: 
    - Ưu tiên dùng ChromeDriver có sẵn trong PATH để tránh download chậm
    - Thêm timeout cho việc tạo driver để tránh treo lâu
    
    Args:
        headless: Chạy headless mode hay không
        timeout: Timeout cho việc tạo driver (giây), mặc định 60s
    
    Returns:
        WebDriver object hoặc None nếu Selenium không có
    """
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service
        import threading
        
        chrome_options = get_selenium_options(headless)
        if not chrome_options:
            return None
        
        driver = None
        driver_created = threading.Event()
        error_occurred = [None]  # Dùng list để có thể modify trong nested function
        
        def create_driver():
            """Helper function để tạo driver trong thread riêng"""
            nonlocal driver
            try:
                # Ưu tiên 1: Thử dùng ChromeDriver có sẵn trong PATH (nhanh nhất)
                try:
                    driver = webdriver.Chrome(options=chrome_options)
                    driver_created.set()
                except Exception as e:
                    # Nếu không có ChromeDriver trong PATH, thử webdriver-manager
                    # Nhưng chỉ thử nếu lỗi là về driver không tìm thấy
                    error_msg = str(e).lower()
                    if 'chromedriver' not in error_msg and 'driver' not in error_msg:
                        # Lỗi khác, không phải về driver -> raise
                        error_occurred[0] = e
                        driver_created.set()
                        return
                    
                    # Ưu tiên 2: Thử dùng webdriver-manager (chậm hơn, có thể download)
                    try:
                        from webdriver_manager.chrome import ChromeDriverManager
                        # Tắt log của webdriver_manager để giảm noise
                        import logging
                        wdm_logger = logging.getLogger('WDM')
                        wdm_logger.setLevel(logging.WARNING)
                        
                        # Install với cache để tránh download lại
                        service = Service(ChromeDriverManager().install())
                        driver = webdriver.Chrome(service=service, options=chrome_options)
                        driver_created.set()
                    except ImportError:
                        # Không có webdriver-manager, raise lỗi gốc
                        error_occurred[0] = e
                        driver_created.set()
            except Exception as e:
                error_occurred[0] = e
                driver_created.set()
        
        # Tạo driver trong thread riêng với timeout
        thread = threading.Thread(target=create_driver, daemon=True)
        thread.start()
        
        # Đợi với timeout
        if driver_created.wait(timeout=timeout):
            if error_occurred[0]:
                raise error_occurred[0]
            return driver
        else:
            # Timeout - thread vẫn đang chạy nhưng quá lâu
            raise TimeoutError(f"Tạo Selenium driver timeout sau {timeout} giây")
            
    except ImportError:
        return None


# ============================================================================
# Sales Count Parsing (shared logic)
# ============================================================================
def parse_sales_count(value: Union[str, int, float, Dict, None]) -> Optional[int]:
    """
    Parse sales_count từ nhiều format khác nhau
    
    Args:
        value: Giá trị có thể là string, int, float, dict, hoặc None
        
    Returns:
        int hoặc None
    """
    if value is None:
        return None
    
    # Nếu là dict (có thể có 'text' và 'value')
    if isinstance(value, dict):
        # Ưu tiên 'value' nếu có
        if 'value' in value:
            value = value['value']
        elif 'text' in value:
            value = value['text']
        else:
            return None
    
    # Nếu là số, trả về int
    if isinstance(value, (int, float)):
        return int(value)
    
    # Nếu là string, parse số
    if isinstance(value, str):
        # Tìm số với đơn vị (ví dụ: "2k" -> 2000, "1.5k" -> 1500)
        sales_match = re.search(r'([\d.]+)\s*([km]?)', value.lower())
        if sales_match:
            num = float(sales_match.group(1))
            unit = sales_match.group(2)
            if unit == 'k':
                return int(num * 1000)
            elif unit == 'm':
                return int(num * 1000000)
            else:
                return int(num)
        else:
            # Thử parse số trực tiếp
            try:
                return int(re.sub(r'[^\d]', '', value))
            except:
                return None
    
    return None


# ============================================================================
# Price Parsing (shared logic)
# ============================================================================
def parse_price(price_text: Optional[str]) -> Optional[int]:
    """
    Parse giá từ text (ví dụ: '389.000₫' -> 389000)
    
    Args:
        price_text: Text chứa giá
        
    Returns:
        int hoặc None
    """
    if not price_text:
        return None
    
    # Loại bỏ ký tự không phải số
    price_clean = re.sub(r'[^\d]', '', str(price_text))
    try:
        return int(price_clean) if price_clean else None
    except:
        return None


# ============================================================================
# File Operations (optimized)
# ============================================================================
def ensure_dir(path: Union[str, Path]) -> Path:
    """Đảm bảo thư mục tồn tại"""
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj


def atomic_write_json(filepath: Union[str, Path], data: Any, **kwargs) -> bool:
    """
    Ghi JSON file một cách atomic (tránh corrupt file)
    
    Args:
        filepath: Đường dẫn file
        data: Dữ liệu cần ghi
        **kwargs: Các tham số cho json.dump
        
    Returns:
        True nếu thành công, False nếu lỗi
    """
    filepath = Path(filepath)
    temp_file = filepath.with_suffix('.tmp')
    
    try:
        # Đảm bảo thư mục tồn tại
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Ghi vào temp file
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2, **kwargs)
        
        # Atomic move
        if os.name == 'nt':  # Windows
            if filepath.exists():
                filepath.unlink()
            import shutil
            shutil.move(str(temp_file), str(filepath))
        else:  # Unix/Linux
            os.rename(str(temp_file), str(filepath))
        
        return True
    except Exception as e:
        # Xóa temp file nếu có lỗi
        try:
            if temp_file.exists():
                temp_file.unlink()
        except:
            pass
        return False


def safe_read_json(filepath: Union[str, Path], default: Any = None) -> Any:
    """
    Đọc JSON file một cách an toàn
    
    Args:
        filepath: Đường dẫn file
        default: Giá trị mặc định nếu lỗi
        
    Returns:
        Dữ liệu JSON hoặc default
    """
    filepath = Path(filepath)
    if not filepath.exists():
        return default
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except:
        return default


# ============================================================================
# URL Utilities
# ============================================================================
def extract_product_id_from_url(url: str) -> Optional[str]:
    """
    Extract product ID từ URL Tiki
    
    Args:
        url: URL sản phẩm
        
    Returns:
        Product ID hoặc None
    """
    # Pattern: /p{product_id}.html hoặc -p{product_id}.html hoặc /p/{product_id}
    match = re.search(r'[\/-]p[/-]?(\d+)', url)
    if match:
        return match.group(1)
    return None


def normalize_url(url: str, base_url: str = 'https://tiki.vn') -> str:
    """
    Chuẩn hóa URL
    
    Args:
        url: URL cần chuẩn hóa
        base_url: Base URL mặc định
        
    Returns:
        URL đã chuẩn hóa
    """
    if not url:
        return ''
    
    if url.startswith('//'):
        return 'https:' + url
    elif url.startswith('/'):
        from urllib.parse import urljoin
        return urljoin(base_url, url)
    elif url.startswith('http'):
        return url
    else:
        from urllib.parse import urljoin
        return urljoin(base_url, url)


# ============================================================================
# Rate Limiting
# ============================================================================
class RateLimiter:
    """Rate limiter đơn giản để tránh quá tải server"""
    
    def __init__(self, delay: float = 1.0):
        """
        Args:
            delay: Thời gian delay giữa các request (giây)
        """
        self.delay = delay
        self.last_request_time = 0.0
    
    def wait(self):
        """Chờ đến khi đủ thời gian delay"""
        current_time = time.time()
        elapsed = current_time - self.last_request_time
        if elapsed < self.delay:
            time.sleep(self.delay - elapsed)
        self.last_request_time = time.time()


# ============================================================================
# Constants
# ============================================================================
# Default paths
DEFAULT_DATA_DIR = Path('data/raw')
DEFAULT_CACHE_DIR = DEFAULT_DATA_DIR / 'cache'
DEFAULT_PRODUCTS_DIR = DEFAULT_DATA_DIR / 'products'
DEFAULT_DETAIL_CACHE_DIR = DEFAULT_PRODUCTS_DIR / 'detail' / 'cache'

# Default user agent
DEFAULT_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# Default timeout
DEFAULT_TIMEOUT = 30

