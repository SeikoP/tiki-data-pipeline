"""
Shared utilities cho crawl pipeline
Tối ưu: loại bỏ code duplication, cải thiện performance
"""

import json
import os
import re
import sys
import threading
import time
from pathlib import Path
from typing import Any


# ============================================================================
# UTF-8 Encoding Setup (tránh lặp lại ở nhiều file)
# ============================================================================
def setup_utf8_encoding():
    """Setup UTF-8 encoding cho stdout trên Windows"""
    if sys.platform == "win32":
        try:
            if hasattr(sys.stdout, "buffer") and not sys.stdout.closed:
                sys.stdout.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            try:
                import io

                if hasattr(sys.stdout, "buffer"):
                    sys.stdout = io.TextIOWrapper(
                        sys.stdout.buffer, encoding="utf-8", errors="replace"
                    )
            except Exception:
                pass


# ============================================================================
# Selenium Setup (shared configuration)
# ============================================================================
def get_selenium_options(headless: bool = True) -> Any | None:
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
            chrome_options.add_argument("--headless=new")  # Use new headless mode (faster)
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-software-rasterizer")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")  # Block plugins
        chrome_options.add_argument("--disable-infobars")  # Disable info bars
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
        chrome_options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        # Performance optimizations - Block unnecessary resources
        prefs = {
            "profile.managed_default_content_settings.images": 2,  # Block images for faster loading
            "profile.default_content_setting_values.stylesheets": 2,  # Block CSS for faster loading
            "profile.default_content_setting_values.notifications": 2,
            "profile.default_content_setting_values.media_stream": 2,  # Block media streams
            "profile.default_content_setting_values.plugins": 2,  # Block plugins
            "disk-cache-size": 4096,  # Limit cache size
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # Faster page load: don't wait for all resources
        try:
            chrome_options.page_load_strategy = "eager"
        except Exception:
            # Older Selenium versions may not support the attribute
            pass

        # Additional performance flags
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-notifications")
        return chrome_options
    except ImportError:
        return None


def create_selenium_driver(headless: bool = True, timeout: int = 60) -> Any | None:
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
        import threading

        from selenium import webdriver
        from selenium.webdriver.chrome.service import Service

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
                    if "chromedriver" not in error_msg and "driver" not in error_msg:
                        # Lỗi khác, không phải về driver -> raise
                        error_occurred[0] = e
                        driver_created.set()
                        return

                    # Ưu tiên 2: Thử dùng webdriver-manager (chậm hơn, có thể download)
                    try:
                        # Tắt log của webdriver_manager để giảm noise
                        import logging
                        import stat

                        from webdriver_manager.chrome import ChromeDriverManager

                        wdm_logger = logging.getLogger("WDM")
                        wdm_logger.setLevel(logging.WARNING)

                        # Install với cache để tránh download lại
                        driver_path = ChromeDriverManager().install()
                        
                        # QUAN TRỌNG: Set quyền thực thi cho ChromeDriver (fix lỗi status code 127)
                        # Đặc biệt cần thiết trong WSL2/Linux
                        try:
                            os.chmod(driver_path, os.stat(driver_path).st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
                        except Exception:
                            pass  # Nếu không set được quyền, vẫn thử tiếp
                        
                        # Kiểm tra ChromeDriver có thể chạy được không (kiểm tra dependencies)
                        try:
                            import subprocess
                            result = subprocess.run(
                                [driver_path, "--version"],
                                capture_output=True,
                                timeout=5,
                                text=True
                            )
                            if result.returncode != 0:
                                raise RuntimeError(
                                    f"ChromeDriver không thể chạy được. "
                                    f"Có thể thiếu dependencies (libnss3, libnspr4, etc.). "
                                    f"Chạy: bash scripts/install_chromedriver_dependencies.sh"
                                )
                        except (subprocess.TimeoutExpired, FileNotFoundError, RuntimeError) as check_error:
                            # Nếu kiểm tra fail, vẫn thử tiếp (có thể là vấn đề khác)
                            pass
                        except Exception:
                            pass
                        
                        service = Service(driver_path)
                        driver = webdriver.Chrome(service=service, options=chrome_options)
                        driver_created.set()
                    except ImportError:
                        # Không có webdriver-manager, raise lỗi gốc
                        error_occurred[0] = e
                        driver_created.set()
                    except Exception as wdm_error:
                        # Cải thiện error message cho lỗi status code 127 (thiếu dependencies)
                        error_msg = str(wdm_error)
                        if "127" in error_msg or "unexpectedly exited" in error_msg.lower():
                            enhanced_error = RuntimeError(
                                f"ChromeDriver không thể khởi động (status code 127). "
                                f"Thiếu dependencies hệ thống (libnss3, libnspr4, etc.).\n"
                                f"Giải pháp: Chạy script sau để cài đặt dependencies:\n"
                                f"  bash scripts/install_chromedriver_dependencies.sh\n"
                                f"Lỗi gốc: {error_msg}"
                            )
                            error_occurred[0] = enhanced_error
                        else:
                            error_occurred[0] = wdm_error
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
# Selenium Driver Pool (for batch processing optimization)
# ============================================================================
class SeleniumDriverPool:
    """Thread-safe pool để reuse Selenium WebDriver instances

    Tối ưu: Giảm overhead tạo driver bằng cách reuse drivers trong batch processing
    """

    def __init__(self, pool_size: int = 5, headless: bool = True, timeout: int = 60):
        """
        Args:
            pool_size: Số lượng drivers tối đa trong pool
            headless: Chạy headless mode hay không
            timeout: Timeout cho việc tạo driver (giây)
        """
        self.pool_size = pool_size
        self.headless = headless
        self.timeout = timeout
        self.pool: list[Any] = []
        self.lock = threading.Lock()
        self.created_count = 0  # Track số lượng drivers đã tạo

    def get_driver(self) -> Any | None:
        """Lấy driver từ pool hoặc tạo mới nếu pool rỗng

        Returns:
            WebDriver object hoặc None nếu không thể tạo
        """
        with self.lock:
            if self.pool:
                driver = self.pool.pop()
                # Kiểm tra driver còn hoạt động không
                try:
                    # Test driver bằng cách lấy current_url
                    _ = driver.current_url
                    return driver
                except Exception:
                    # Driver đã bị close hoặc corrupt, tạo mới
                    try:
                        driver.quit()
                    except Exception:
                        pass
                    # Fall through để tạo driver mới

            # Pool rỗng hoặc driver không hợp lệ, tạo mới
            if self.created_count < self.pool_size * 2:  # Cho phép tạo thêm một chút
                driver = create_selenium_driver(headless=self.headless, timeout=self.timeout)
                if driver:
                    self.created_count += 1
                return driver
            else:
                # Đã tạo quá nhiều drivers, không tạo thêm
                return None

    def return_driver(self, driver: Any) -> None:
        """Trả driver về pool hoặc đóng nếu pool đầy

        Args:
            driver: WebDriver object cần trả về pool
        """
        if not driver:
            return

        with self.lock:
            # Kiểm tra driver còn hoạt động không
            try:
                _ = driver.current_url
            except Exception:
                # Driver đã bị close, không trả về pool
                try:
                    driver.quit()
                except Exception:
                    pass
                return

            # Trả về pool nếu còn chỗ
            if len(self.pool) < self.pool_size:
                self.pool.append(driver)
            else:
                # Pool đầy, đóng driver
                try:
                    driver.quit()
                except Exception:
                    pass

    def cleanup(self) -> None:
        """Đóng tất cả drivers trong pool"""
        with self.lock:
            for driver in self.pool:
                try:
                    driver.quit()
                except Exception:
                    pass
            self.pool.clear()
            self.created_count = 0

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """Context manager exit - cleanup pool"""
        self.cleanup()


# ============================================================================
# Sales Count Parsing (shared logic)
# ============================================================================
def parse_sales_count(value: str | int | float | dict | None) -> int | None:
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
        if "value" in value:
            value = value["value"]
        elif "text" in value:
            value = value["text"]
        else:
            return None

    # Nếu là số, trả về int
    if isinstance(value, (int, float)):
        return int(value)

    # Nếu là string, parse số
    if isinstance(value, str):
        # Tìm số với đơn vị (ví dụ: "2k" -> 2000, "1.5k" -> 1500)
        sales_match = re.search(r"([\d.]+)\s*([km]?)", value.lower())
        if sales_match:
            num = float(sales_match.group(1))
            unit = sales_match.group(2)
            if unit == "k":
                return int(num * 1000)
            elif unit == "m":
                return int(num * 1000000)
            else:
                return int(num)
        else:
            # Thử parse số trực tiếp
            try:
                return int(re.sub(r"[^\d]", "", value))
            except Exception:
                return None

    return None


# ============================================================================
# Price Parsing (shared logic)
# ============================================================================
def parse_price(price_text: str | None) -> int | None:
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
    price_clean = re.sub(r"[^\d]", "", str(price_text))
    try:
        return int(price_clean) if price_clean else None
    except Exception:
        return None


# ============================================================================
# File Operations (optimized)
# ============================================================================
def ensure_dir(path: str | Path) -> Path:
    """Đảm bảo thư mục tồn tại"""
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=True)
    return path_obj


def atomic_write_json(filepath: str | Path, data: Any, compress: bool = False, **kwargs) -> bool:
    """
    Ghi JSON file một cách atomic (tránh corrupt file) với optional compression

    Args:
        filepath: Đường dẫn file
        data: Dữ liệu cần ghi
        compress: Có nén file không (gzip)
        **kwargs: Các tham số cho json.dump

    Returns:
        True nếu thành công, False nếu lỗi
    """
    filepath = Path(filepath)
    temp_file = filepath.with_suffix(".tmp")

    try:
        # Đảm bảo thư mục tồn tại
        filepath.parent.mkdir(parents=True, exist_ok=True)

        if compress:
            # Sử dụng compression
            try:
                from ..storage.compression import write_compressed_json

                if write_compressed_json(temp_file, data):
                    # Atomic move cho compressed file
                    if os.name == "nt":  # Windows
                        if filepath.exists():
                            filepath.unlink()
                        import shutil

                        shutil.move(str(temp_file), str(filepath))
                    else:  # Unix/Linux
                        os.rename(str(temp_file), str(filepath))
                    return True
                else:
                    compress = False  # Fallback nếu compression fail
            except ImportError:
                # Fallback về normal write nếu không có compression module
                compress = False

        if not compress:
            # Ghi vào temp file
            with open(temp_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2, **kwargs)

        # Atomic move
        if os.name == "nt":  # Windows
            if filepath.exists():
                filepath.unlink()
            import shutil

            shutil.move(str(temp_file), str(filepath))
        else:  # Unix/Linux
            os.rename(str(temp_file), str(filepath))

        return True
    except Exception:
        # Xóa temp file nếu có lỗi
        try:
            if temp_file.exists():
                temp_file.unlink()
        except Exception:
            pass
        return False


def safe_read_json(filepath: str | Path, default: Any = None, try_compressed: bool = True) -> Any:
    """
    Đọc JSON file một cách an toàn với support cho compressed files

    Args:
        filepath: Đường dẫn file
        default: Giá trị mặc định nếu lỗi
        try_compressed: Thử đọc compressed version (.gz) nếu file không tồn tại

    Returns:
        Dữ liệu JSON hoặc default
    """
    filepath = Path(filepath)

    # Thử đọc compressed version trước
    if try_compressed:
        try:
            from ..storage.compression import read_compressed_json

            result = read_compressed_json(filepath, default=None)
            if result is not None:
                return result
        except ImportError:
            pass

    # Fallback về normal read
    if not filepath.exists():
        return default

    try:
        with open(filepath, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


# ============================================================================
# URL Utilities
# ============================================================================
def extract_product_id_from_url(url: str) -> str | None:
    """
    Extract product ID từ URL Tiki

    Args:
        url: URL sản phẩm

    Returns:
        Product ID hoặc None
    """
    # Pattern: /p{product_id}.html hoặc -p{product_id}.html hoặc /p/{product_id}
    match = re.search(r"[\/-]p[/-]?(\d+)", url)
    if match:
        return match.group(1)
    return None


def extract_category_id_from_url(url: str) -> str | None:
    """
    Extract category ID từ URL Tiki

    Args:
        url: URL danh mục (ví dụ: https://tiki.vn/amplifier/c68289)

    Returns:
        Category ID (ví dụ: c68289) hoặc None
    """
    if not url:
        return None
    # Pattern: /slug/c{category_id} hoặc /slug/c{category_id}?...
    match = re.search(r"/c(\d+)", url)
    if match:
        return f"c{match.group(1)}"
    return None


def normalize_url(url: str, base_url: str = "https://tiki.vn") -> str:
    """
    Chuẩn hóa URL

    Args:
        url: URL cần chuẩn hóa
        base_url: Base URL mặc định

    Returns:
        URL đã chuẩn hóa
    """
    if not url:
        return ""

    if url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        from urllib.parse import urljoin

        return urljoin(base_url, url)
    elif url.startswith("http"):
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
DEFAULT_DATA_DIR = Path("data/raw")
DEFAULT_CACHE_DIR = DEFAULT_DATA_DIR / "cache"
DEFAULT_PRODUCTS_DIR = DEFAULT_DATA_DIR / "products"
DEFAULT_DETAIL_CACHE_DIR = DEFAULT_PRODUCTS_DIR / "detail" / "cache"

# Default user agent
DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

# Default timeout
DEFAULT_TIMEOUT = 30
