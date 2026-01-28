# Lazy import Selenium và BeautifulSoup để tránh timeout khi load DAG
# from selenium import webdriver
# from selenium.webdriver.common.by import By
# from selenium.webdriver.support.ui import WebDriverWait
# from selenium.webdriver.support import expected_conditions as EC
# from bs4 import BeautifulSoup
import json
import re
import time
from datetime import datetime
from typing import Any

# Import shared utilities - hỗ trợ cả relative và absolute import
try:
    # Thử relative import trước (khi chạy như package)
    from .utils import (
        atomic_write_json,
        create_selenium_driver,
        ensure_dir,
        extract_product_id_from_url,
        parse_price,
        parse_sales_count,
        setup_utf8_encoding,
    )
except ImportError:
    # Fallback: absolute import (khi được load qua importlib)
    import os

    # Tìm utils.py trong cùng thư mục
    current_dir = os.path.dirname(os.path.abspath(__file__))
    utils_path = os.path.join(current_dir, "utils.py")
    if os.path.exists(utils_path):
        import importlib.util

        spec = importlib.util.spec_from_file_location("crawl_utils", utils_path)
        if spec and spec.loader:
            utils_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(utils_module)
            setup_utf8_encoding = utils_module.setup_utf8_encoding
            parse_sales_count = utils_module.parse_sales_count
            parse_price = utils_module.parse_price
            extract_product_id_from_url = utils_module.extract_product_id_from_url
            create_selenium_driver = utils_module.create_selenium_driver
            atomic_write_json = utils_module.atomic_write_json
            ensure_dir = utils_module.ensure_dir
        else:
            raise ImportError(f"Không thể load utils từ {utils_path}") from None
    else:
        raise ImportError(f"Không tìm thấy utils.py tại {utils_path}") from None

# Setup UTF-8 encoding
setup_utf8_encoding()


def crawl_product_detail_with_selenium(
    url,
    save_html=False,
    verbose=True,
    max_retries=2,
    timeout=30,
    use_redis_cache=True,
    use_rate_limiting=True,
    wait_config_overrides: dict | None = None,
):
    """Crawl trang sản phẩm Tiki bằng Selenium để load đầy đủ dữ liệu

    Args:
        url: URL sản phẩm cần crawl
        save_html: Có lưu HTML vào file không
        verbose: Có in log không
        max_retries: Số lần retry tối đa
        timeout: Timeout cho page load (giây)
        use_redis_cache: Có dùng Redis cache không
        use_rate_limiting: Có dùng rate limiting không

    Returns:
        str: HTML content hoặc None nếu lỗi

    Raises:
        Exception: Nếu không thể crawl sau max_retries lần
    """
    # Thử Redis cache trước
    if use_redis_cache:
        try:
            from pipelines.crawl.storage.redis_cache import get_redis_cache

            redis_cache = get_redis_cache("redis://redis:6379/1")
            if redis_cache:
                cached_html = redis_cache.get_cached_html(url)
                if cached_html:
                    if verbose:
                        print(f"[Redis Cache] ✅ Hit cache cho {url[:60]}...")
                    return cached_html
        except Exception:
            pass  # Fallback về crawl

    # Adaptive Rate Limiting - tự động điều chỉnh delay
    adaptive_limiter = None
    if use_rate_limiting:
        try:
            from urllib.parse import urlparse

            from pipelines.crawl.storage.adaptive_rate_limiter import get_adaptive_rate_limiter

            adaptive_limiter = get_adaptive_rate_limiter("redis://redis:6379/2")
            if adaptive_limiter:
                domain = urlparse(url).netloc or "tiki.vn"
                # Đợi với adaptive delay (tự động điều chỉnh)
                if verbose:
                    current_delay = adaptive_limiter.get_current_delay()
                    print(f"[Adaptive Rate Limiter] ⏳ Delay: {current_delay:.2f}s cho {domain}")
                adaptive_limiter.wait(domain)
        except Exception as e:
            # Fallback về fixed delay nếu adaptive limiter không available
            if verbose:
                print(
                    f"[Rate Limiter] ⚠️  Adaptive limiter không available: {e}, dùng delay cố định"
                )
            time.sleep(0.7)  # Fixed delay fallback

    driver = None
    last_error = None

    for attempt in range(max_retries):
        try:
            # Sử dụng shared utility để tạo driver
            # Tăng timeout lên 120s để tránh timeout khi có nhiều tasks chạy song song
            driver = create_selenium_driver(headless=True, timeout=120)
            if not driver:
                raise ImportError("Selenium chưa được cài đặt hoặc không thể tạo driver")

            # Set timeout cho page load
            driver.set_page_load_timeout(timeout)
            # Tier 1: Moderate wait time increase for better data capture
            implicit_wait = 4  # Default fallback
            try:
                from .config import CRAWL_IMPLICIT_WAIT_NORMAL

                implicit_wait = CRAWL_IMPLICIT_WAIT_NORMAL
            except ImportError:
                pass

            # Apply overrides if present
            if wait_config_overrides and "implicit_wait" in wait_config_overrides:
                implicit_wait = wait_config_overrides["implicit_wait"]

            driver.implicitly_wait(implicit_wait)

            if verbose:
                print(f"[Selenium] Đang mở {url}... (attempt {attempt + 1}/{max_retries})")

            driver.get(url)

            # Smart wait: Chờ page load với explicit wait (thay vì time.sleep fixed)
            # Expected: Giảm wait time từ 1s → 0.3-0.8s (nếu page load nhanh)
            try:
                from .utils_selenium_wait import smart_wait_for_page_load

                page_loaded = smart_wait_for_page_load(
                    driver, check_product_elements=True, timeout=5, verbose=verbose
                )
                if not page_loaded and verbose:
                    print("[Selenium] ⚠️  Page có thể chưa load đầy đủ, nhưng tiếp tục...")
            except ImportError:
                # Fallback về time.sleep nếu module chưa có
                if verbose:
                    print(
                        "[Selenium] ⚠️  Sử dụng time.sleep fallback (explicit waits không available)"
                    )
                time.sleep(1)

            # Scroll để load các phần động - tối ưu với explicit waits
            if verbose:
                print("[Selenium] Đang scroll để load nội dung...")

            # Optimized scrolling với smart waits
            # Scroll để load seller và price information (dynamic content)
            try:
                from .utils_selenium_wait import wait_after_scroll, wait_for_dynamic_content_loaded

                driver.execute_script("window.scrollTo(0, 500);")
                wait_after_scroll(driver, timeout=1, verbose=verbose)  # Wait tối đa 1s

                driver.execute_script("window.scrollTo(0, 1500);")
                wait_after_scroll(driver, timeout=1, verbose=verbose)

                # Scroll đến seller section (thường ở giữa page)
                driver.execute_script("window.scrollTo(0, 2500);")
                wait_after_scroll(driver, timeout=1, verbose=verbose)

                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                # Wait cho dynamic content (seller, price, sales_count, rating) load
                # Tier 1: Increased timeout to ensure seller and original_price load
                # Wait cho dynamic content (seller, price, sales_count, rating) load
                # Tier 1: Increased timeout to ensure seller and original_price load
                dynamic_wait = 4
                scroll_sleep = 1.2

                try:
                    from .config import (
                        CRAWL_DYNAMIC_CONTENT_WAIT_NORMAL,
                        CRAWL_POST_SCROLL_SLEEP_NORMAL,
                    )

                    dynamic_wait = CRAWL_DYNAMIC_CONTENT_WAIT_NORMAL
                    scroll_sleep = CRAWL_POST_SCROLL_SLEEP_NORMAL
                except ImportError:
                    pass

                # Apply overrides if present
                if wait_config_overrides:
                    if "dynamic_content_wait" in wait_config_overrides:
                        dynamic_wait = wait_config_overrides["dynamic_content_wait"]
                    if "post_scroll_sleep" in wait_config_overrides:
                        scroll_sleep = wait_config_overrides["post_scroll_sleep"]

                try:
                    from .utils_selenium_wait import wait_for_dynamic_content_loaded

                    wait_for_dynamic_content_loaded(driver, timeout=dynamic_wait, verbose=verbose)
                    time.sleep(scroll_sleep)
                except ImportError:
                    wait_for_dynamic_content_loaded(driver, timeout=dynamic_wait, verbose=verbose)
                    time.sleep(scroll_sleep)  # Fallback
            except ImportError:
                # Fallback về time.sleep nếu module chưa có
                # Scroll đến seller section và wait lâu hơn
                driver.execute_script("window.scrollTo(0, 2500);")
                time.sleep(1.0)  # Wait lâu hơn cho seller section
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1.5)  # Wait cho dynamic content (seller, price)
            except Exception as scroll_error:
                if verbose:
                    print(f"[Selenium] Warning: Lỗi khi scroll: {scroll_error}")
                # Tiếp tục dù scroll lỗi

            # Lấy HTML đầy đủ
            full_html = driver.page_source

            # Validate HTML
            if not full_html or len(full_html) < 100:
                raise ValueError(
                    f"HTML content quá ngắn: {len(full_html) if full_html else 0} ký tự"
                )

            # Lưu HTML nếu được yêu cầu - dùng shared utility
            if save_html:
                output_dir = ensure_dir("data/test_output")
                html_file = output_dir / "selenium_product_detail.html"
                # Lưu HTML trực tiếp (không phải JSON)
                with open(html_file, "w", encoding="utf-8") as f:
                    f.write(full_html)
                if verbose:
                    print(f"[Selenium] ✓ Đã lưu HTML vào {html_file}")

            # Cache HTML vào Redis sau khi crawl thành công (với canonical URL)
            if use_redis_cache and full_html:
                try:
                    from pipelines.crawl.config import REDIS_CACHE_TTL_HTML
                    from pipelines.crawl.storage.redis_cache import get_redis_cache

                    redis_cache = get_redis_cache("redis://redis:6379/1")
                    if redis_cache:
                        # CRITICAL: Chuẩn hóa URL trước khi cache để maximize hit rate
                        canonical_url = redis_cache._canonicalize_url(url)
                        redis_cache.cache_html(canonical_url, full_html, ttl=REDIS_CACHE_TTL_HTML)
                        if verbose:
                            print(
                                f"[Redis Cache] ✅ Đã cache HTML với TTL={REDIS_CACHE_TTL_HTML}s (7 days) cho {url[:60]}..."
                            )
                except Exception:
                    pass  # Ignore cache errors

            # Đóng driver trước khi return
            if driver:
                driver.quit()
                driver = None

            # Record success cho adaptive rate limiter
            if adaptive_limiter:
                try:
                    from urllib.parse import urlparse

                    domain = urlparse(url).netloc or "tiki.vn"
                    adaptive_limiter.record_success(domain)
                except Exception:
                    pass  # Ignore errors

            return full_html

        except Exception as e:
            last_error = e
            error_type = type(e).__name__
            error_msg = str(e)

            # Record error cho adaptive rate limiter
            if adaptive_limiter:
                try:
                    from urllib.parse import urlparse

                    domain = urlparse(url).netloc or "tiki.vn"
                    # Detect error type
                    error_type_str = None
                    if "429" in str(e) or "Too Many Requests" in str(e):
                        error_type_str = "429"
                    elif "timeout" in str(e).lower() or "Timeout" in error_type:
                        error_type_str = "timeout"
                    adaptive_limiter.record_error(domain, error_type=error_type_str)
                except Exception:
                    pass  # Ignore errors

            if verbose:
                print(
                    f"[Selenium] Lỗi attempt {attempt + 1}/{max_retries} ({error_type}): {error_msg}"
                )

            # Đóng driver nếu có lỗi
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = None

            # Nếu là lỗi không thể retry (như ImportError), raise ngay
            if isinstance(e, ImportError):
                raise

            # Nếu đã hết retries, raise lỗi
            if attempt == max_retries - 1:
                raise Exception(
                    f"Không thể crawl sau {max_retries} lần thử. Lỗi cuối: {error_type}: {error_msg}"
                ) from e

            # Chờ một chút trước khi retry (tăng delay để giảm tải hệ thống)
            retry_delay = (attempt + 1) * 3  # Exponential backoff: 3s, 6s, 9s...
            if verbose:
                print(f"[Selenium] Chờ {retry_delay}s trước khi retry...")
            time.sleep(retry_delay)

    # Không bao giờ đến đây, nhưng để an toàn
    if driver:
        try:
            driver.quit()
        except Exception:
            pass

    raise (
        Exception(f"Không thể crawl. Lỗi: {last_error}")
        if last_error
        else Exception("Không thể crawl")
    )


def crawl_product_with_retry(
    url: str,
    max_retries: int | None = None,
    required_fields: list[str] | None = None,
    verbose: bool = True,
    use_redis_cache: bool = True,
    use_rate_limiting: bool = True,
) -> dict[str, Any] | None:
    """
    Crawl product with conditional retry for missing critical fields.

    This function implements the Hybrid Approach:
    - Tier 1: Normal crawl with moderate wait times
    - Tier 2: Retry with increased wait times if important fields missing
    - Tier 3: Accept or skip based on data quality after max retries

    Args:
        url: Product URL to crawl
        max_retries: Maximum retry attempts (default: from config)
        required_fields: Fields that must be present (default: from config)
        verbose: Enable logging
        use_redis_cache: Use Redis cache for HTML
        use_rate_limiting: Use adaptive rate limiting

    Returns:
        Product data dict with metadata, or None if failed/skipped
    """
    import time
    from datetime import datetime

    # Import config and validator
    try:
        from .config import PRODUCT_RETRY_DELAY_BASE, PRODUCT_RETRY_MAX_ATTEMPTS
        from .data_validator import enrich_product_metadata, validate_product_data
    except ImportError:
        # Fallback defaults
        PRODUCT_RETRY_MAX_ATTEMPTS = 2
        PRODUCT_RETRY_DELAY_BASE = 3

        def validate_product_data(product, retry_count=0):
            return "accept"

        def enrich_product_metadata(product, retry_count=0, crawl_status="success"):
            return product

    if max_retries is None:
        max_retries = PRODUCT_RETRY_MAX_ATTEMPTS

    retry_count = 0
    last_product = None
    crawl_start_time = datetime.now()

    for attempt in range(max_retries + 1):
        try:
            attempt_start_time = time.time()

            if verbose:
                tier = "Tier 1 (Normal)" if attempt == 0 else f"Tier 2 (Retry {attempt})"
                print(f"\n[Retry Wrapper] {tier} - Crawling {url[:60]}...")

            # Crawl HTML
            if attempt == 0:
                # Tier 1: Normal wait times
                html = crawl_product_detail_with_selenium(
                    url,
                    save_html=False,
                    verbose=verbose,
                    max_retries=1,  # Internal retries
                    timeout=120,
                    use_redis_cache=use_redis_cache,
                    use_rate_limiting=use_rate_limiting,
                )
            else:
                # Tier 2: Increased wait times for retry
                # Define overrides directly
                retry_overrides = {
                    "implicit_wait": 8,  # Default retry value
                    "dynamic_content_wait": 8,
                    "post_scroll_sleep": 2.4,
                }

                # Try to load values from config if available (optional)
                try:
                    from .config import (
                        CRAWL_DYNAMIC_CONTENT_WAIT_RETRY,
                        CRAWL_IMPLICIT_WAIT_RETRY,
                        CRAWL_POST_SCROLL_SLEEP_RETRY,
                    )

                    retry_overrides["implicit_wait"] = CRAWL_IMPLICIT_WAIT_RETRY
                    retry_overrides["dynamic_content_wait"] = CRAWL_DYNAMIC_CONTENT_WAIT_RETRY
                    retry_overrides["post_scroll_sleep"] = CRAWL_POST_SCROLL_SLEEP_RETRY
                except ImportError:
                    pass

                html = crawl_product_detail_with_selenium(
                    url,
                    save_html=False,
                    verbose=verbose,
                    max_retries=1,
                    timeout=120,
                    use_redis_cache=False,  # Don't use cache on retry
                    use_rate_limiting=use_rate_limiting,
                    wait_config_overrides=retry_overrides,
                )

            if not html:
                raise ValueError("Failed to get HTML content")

            # Extract product data
            product = extract_product_detail(html, url, verbose=verbose)

            if not product:
                raise ValueError("Failed to extract product data")

            # Calculate crawl duration
            crawl_duration_ms = int((time.time() - attempt_start_time) * 1000)

            # Validate product data
            action = validate_product_data(product, retry_count=retry_count)

            if verbose:
                print(f"[Retry Wrapper] Validation result: {action}")
                if action == "retry":
                    missing = product.get("_metadata", {}).get("missing_fields", [])
                    print(f"[Retry Wrapper] Missing fields: {missing}")

            if action == "accept":
                # Enrich with metadata
                product = enrich_product_metadata(
                    product, retry_count=retry_count, crawl_status="success"
                )
                # Add timing metadata
                if "_metadata" not in product:
                    product["_metadata"] = {}
                product["_metadata"]["crawl_duration_ms"] = crawl_duration_ms
                product["_metadata"]["started_at"] = crawl_start_time.isoformat()
                product["_metadata"]["completed_at"] = datetime.now().isoformat()

                if verbose:
                    score = product["_metadata"].get("data_completeness_score", 0)
                    print(f"[Retry Wrapper] ✅ Accepted with score: {score}")

                last_product = product
                return product

            elif action == "skip":
                if verbose:
                    print("[Retry Wrapper] ❌ Skipping product (missing critical fields)")
                return None

            elif action == "retry":
                if retry_count < max_retries:
                    retry_count += 1
                    last_product = product

                    # Wait before retry (exponential backoff)
                    retry_delay = PRODUCT_RETRY_DELAY_BASE * (attempt + 1)
                    if verbose:
                        print(
                            f"[Retry Wrapper] ⏳ Waiting {retry_delay}s before retry {retry_count}..."
                        )
                    time.sleep(retry_delay)
                    continue
                else:
                    # Max retries reached - accept with partial data
                    product = enrich_product_metadata(
                        product, retry_count=retry_count, crawl_status="partial"
                    )
                    product["_metadata"]["crawl_duration_ms"] = crawl_duration_ms
                    product["_metadata"]["started_at"] = crawl_start_time.isoformat()
                    product["_metadata"]["completed_at"] = datetime.now().isoformat()

                    if verbose:
                        score = product["_metadata"].get("data_completeness_score", 0)
                        print(
                            f"[Retry Wrapper] ⚠️  Accepted with partial data after {max_retries} retries (score: {score})"
                        )

                    last_product = product
                    return product

        except Exception as e:
            if verbose:
                print(f"[Retry Wrapper] Error on attempt {attempt + 1}: {type(e).__name__}: {e}")

            if attempt < max_retries:
                retry_count += 1
                retry_delay = PRODUCT_RETRY_DELAY_BASE * (attempt + 1)
                if verbose:
                    print(f"[Retry Wrapper] ⏳ Waiting {retry_delay}s before retry...")
                time.sleep(retry_delay)
                continue
            else:
                # Failed after all retries
                if verbose:
                    print(f"[Retry Wrapper] ❌ Failed after {max_retries + 1} attempts")

                # Return last product with error metadata if available
                if last_product:
                    last_product = enrich_product_metadata(
                        last_product, retry_count=retry_count, crawl_status="failed"
                    )
                    if "_metadata" not in last_product:
                        last_product["_metadata"] = {}
                    last_product["_metadata"]["error_message"] = str(e)
                    last_product["_metadata"]["started_at"] = crawl_start_time.isoformat()
                    last_product["_metadata"]["completed_at"] = datetime.now().isoformat()
                    return last_product

                return None

    return None


def crawl_product_detail_with_driver(
    driver: Any,
    url: str,
    save_html: bool = False,
    verbose: bool = True,
    timeout: int = 30,
    use_redis_cache: bool = True,
    use_rate_limiting: bool = True,
) -> str | None:
    """Crawl trang sản phẩm sử dụng Selenium driver đã được cấp sẵn (reuse/pool).

    Args:
        driver: Selenium WebDriver đã được tạo sẵn
        url: URL sản phẩm cần crawl
        save_html: Có lưu HTML vào file không
        verbose: Có in log không
        timeout: Timeout cho page load (giây)
        use_redis_cache: Có dùng Redis cache không
        use_rate_limiting: Có dùng rate limiting không

    Returns:
        str | None: HTML content hoặc None nếu lỗi
    """
    # Redis cache trước
    if use_redis_cache:
        try:
            from pipelines.crawl.storage.redis_cache import get_redis_cache

            redis_cache = get_redis_cache("redis://redis:6379/1")
            if redis_cache:
                cached_html = redis_cache.get_cached_html(url)
                if cached_html:
                    if verbose:
                        print(f"[Redis Cache] ✅ Hit cache cho {url[:60]}...")
                    return cached_html
        except Exception:
            pass

    # Adaptive Rate Limiting
    adaptive_limiter = None
    if use_rate_limiting:
        try:
            from urllib.parse import urlparse

            from pipelines.crawl.storage.adaptive_rate_limiter import get_adaptive_rate_limiter

            adaptive_limiter = get_adaptive_rate_limiter("redis://redis:6379/2")
            if adaptive_limiter:
                domain = urlparse(url).netloc or "tiki.vn"
                if verbose:
                    current_delay = adaptive_limiter.get_current_delay()
                    print(f"[Adaptive Rate Limiter] ⏳ Delay: {current_delay:.2f}s cho {domain}")
                adaptive_limiter.wait(domain)
        except Exception as e:
            if verbose:
                print(
                    f"[Rate Limiter] ⚠️  Adaptive limiter không available: {e}, dùng delay cố định"
                )
            time.sleep(0.7)  # Fixed delay fallback

    try:
        # Tier 1: Moderate wait time increase
        try:
            from .config import CRAWL_IMPLICIT_WAIT_NORMAL

            driver.implicitly_wait(CRAWL_IMPLICIT_WAIT_NORMAL)  # 4s
        except ImportError:
            driver.implicitly_wait(4)  # Fallback
        if verbose:
            print(f"[Selenium] Đang mở {url}... (reuse driver)")
        driver.get(url)

        # Smart wait: Chờ page load với explicit wait
        try:
            from .utils_selenium_wait import smart_wait_for_page_load

            smart_wait_for_page_load(
                driver, check_product_elements=True, timeout=5, verbose=verbose
            )
        except ImportError:
            time.sleep(1)  # Fallback

        # Optimized scrolling với smart waits
        # Scroll để load seller và price information (dynamic content)
        try:
            from .utils_selenium_wait import wait_after_scroll, wait_for_dynamic_content_loaded

            driver.execute_script("window.scrollTo(0, 500);")
            wait_after_scroll(driver, timeout=1, verbose=verbose)

            driver.execute_script("window.scrollTo(0, 1500);")
            wait_after_scroll(driver, timeout=1, verbose=verbose)

            # Scroll đến seller section (thường ở giữa page)
            driver.execute_script("window.scrollTo(0, 2500);")
            wait_after_scroll(driver, timeout=1, verbose=verbose)

            # Wait cho dynamic content (seller, price, sales_count, rating) load
            # Tier 1: Increased timeout to ensure seller and original_price load
            try:
                from .config import (
                    CRAWL_DYNAMIC_CONTENT_WAIT_NORMAL,
                    CRAWL_POST_SCROLL_SLEEP_NORMAL,
                )

                wait_for_dynamic_content_loaded(
                    driver, timeout=CRAWL_DYNAMIC_CONTENT_WAIT_NORMAL, verbose=verbose
                )  # 4s
                time.sleep(CRAWL_POST_SCROLL_SLEEP_NORMAL)  # 1.2s
            except ImportError:
                wait_for_dynamic_content_loaded(driver, timeout=4, verbose=verbose)
                time.sleep(1.2)  # Fallback
        except ImportError:
            # Fallback về time.sleep
            driver.execute_script("window.scrollTo(0, 500);")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0, 1500);")
            time.sleep(0.5)
            driver.execute_script("window.scrollTo(0, 2500);")  # Scroll đến seller section
            time.sleep(1.0)  # Wait lâu hơn cho seller section
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.5)  # Wait cho dynamic content (seller, price)
        except Exception as scroll_error:
            if verbose:
                print(f"[Selenium] Warning: Lỗi khi scroll: {scroll_error}")

        full_html = driver.page_source
        if not full_html or len(full_html) < 100:
            raise ValueError(f"HTML content quá ngắn: {len(full_html) if full_html else 0} ký tự")

        if save_html:
            output_dir = ensure_dir("data/test_output")
            html_file = output_dir / "selenium_product_detail.html"
            with open(html_file, "w", encoding="utf-8") as f:
                f.write(full_html)
            if verbose:
                print(f"[Selenium] ✓ Đã lưu HTML vào {html_file}")

        if use_redis_cache and full_html:
            try:
                from pipelines.crawl.storage.redis_cache import get_redis_cache

                redis_cache = get_redis_cache("redis://redis:6379/1")
                if redis_cache:
                    redis_cache.cache_html(url, full_html, ttl=86400)
                    if verbose:
                        print(f"[Redis Cache] ✅ Đã cache HTML cho {url[:60]}...")
            except Exception:
                pass  # Ignore cache errors - caching is optional and shouldn't fail the crawl

        # Record success cho adaptive rate limiter
        if adaptive_limiter:
            try:
                from urllib.parse import urlparse

                domain = urlparse(url).netloc or "tiki.vn"
                adaptive_limiter.record_success(domain)
            except Exception:
                pass

        return full_html

    except Exception as e:
        # Record error cho adaptive rate limiter
        if adaptive_limiter:
            try:
                from urllib.parse import urlparse

                domain = urlparse(url).netloc or "tiki.vn"
                error_type_str = None
                if "429" in str(e) or "Too Many Requests" in str(e):
                    error_type_str = "429"
                elif "timeout" in str(e).lower() or "Timeout" in type(e).__name__:
                    error_type_str = "timeout"
                adaptive_limiter.record_error(domain, error_type=error_type_str)
            except Exception:
                pass

        if verbose:
            print(f"[Selenium] Lỗi khi crawl (reuse driver): {type(e).__name__}: {e}")
        return None


# Sử dụng shared utilities thay vì định nghĩa lại
# extract_product_id_from_url và parse_price đã được import từ utils


def load_category_hierarchy():
    """Load category hierarchy map to auto-detect parent categories"""
    import os

    hierarchy_file = "data/raw/category_hierarchy_map.json"
    if os.path.exists(hierarchy_file):
        try:
            with open(hierarchy_file, encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def get_parent_category_name(category_url, hierarchy_map=None):
    """Get parent category name from hierarchy map

    Args:
        category_url: URL của danh mục (level 1-2)
        hierarchy_map: Pre-loaded hierarchy map (optional)

    Returns:
        str: Parent category name (Level 0) hoặc None
    """
    if hierarchy_map is None:
        hierarchy_map = load_category_hierarchy()

    if not hierarchy_map:
        return None

    cat_info = hierarchy_map.get(category_url)
    if not cat_info:
        return None

    # Lấy parent chain (danh sách các parent URLs từ root)
    parent_chain = cat_info.get("parent_chain", [])
    if parent_chain:
        # Parent đầu tiên (Level 0) là "Nhà cửa - đời sống"
        root_parent = parent_chain[0]
        root_info = hierarchy_map.get(root_parent)
        if root_info:
            return root_info["name"]

    return None


def extract_product_detail(
    html_content, url, verbose=True, parent_category=None, hierarchy_map=None
):
    """Extract thông tin chi tiết sản phẩm từ HTML

    Args:
        html_content: HTML content của trang product
        url: URL của product
        verbose: Có in log không
        parent_category: Danh mục cha (vd: "Nhà Cửa - Đời Sống") để prepend vào category_path
        hierarchy_map: Pre-loaded category hierarchy map (optional)
    """
    # Lazy import để tránh timeout khi load DAG
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_content, "html.parser")
    product_data = {
        "url": url,
        "product_id": extract_product_id_from_url(url),  # Dùng shared utility
        "name": "",
        "price": {
            "current_price": None,
            "original_price": None,
            "discount_percent": None,
            "currency": "VND",
        },
        "rating": {"average": None, "total_reviews": 0, "rating_distribution": {}},
        "seller": {"name": "", "is_official": False, "seller_id": ""},
        "description": "",
        "specifications": {},
        "images": [],
        "category_path": [],
        "brand": "",
        "warranty": "",
        "sales_count": None,  # Số lượng sản phẩm đã bán
        "stock": {"available": True, "quantity": 0, "stock_status": ""},
        "shipping": {"free_shipping": False, "fast_delivery": False, "delivery_time": ""},
        "promotions": [],
        "_metadata": {
            "extracted_at": datetime.now().isoformat(),
            "source_url": url,
            "extraction_method": "selenium",
        },
    }

    # 1. Extract tên sản phẩm
    name_selectors = [
        'h1[data-view-id="pdp_product_name"]',
        "h1.product-name",
        'h1[class*="name"]',
        "h1",
        '[data-view-id*="product_name"]',
    ]
    for selector in name_selectors:
        name_elem = soup.select_one(selector)
        if name_elem:
            product_data["name"] = name_elem.get_text(strip=True)
            break

    # 2. Extract giá
    # Tìm giá hiện tại
    price_selectors = [
        '[data-view-id="pdp_product_price"]',
        ".product-price__current-price",
        '[class*="current-price"]',
        '[class*="price-current"]',
    ]
    for selector in price_selectors:
        price_elem = soup.select_one(selector)
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            product_data["price"]["current_price"] = parse_price(price_text)  # Dùng shared utility
            break

    # Tìm giá gốc
    original_price_selectors = [
        ".product-price__list-price",
        '[class*="original-price"]',
        '[class*="list-price"]',
        ".price-old",
    ]
    for selector in original_price_selectors:
        price_elem = soup.select_one(selector)
        if price_elem:
            price_text = price_elem.get_text(strip=True)
            product_data["price"]["original_price"] = parse_price(price_text)  # Dùng shared utility
            break

    # Tính phần trăm giảm giá
    if product_data["price"]["current_price"] and product_data["price"]["original_price"]:
        discount = (
            (product_data["price"]["original_price"] - product_data["price"]["current_price"])
            / product_data["price"]["original_price"]
        ) * 100
        product_data["price"]["discount_percent"] = round(discount, 1)

    # 3. Extract đánh giá
    rating_selectors = [
        '[data-view-id="pdp_rating_score"]',
        ".rating-score",
        '[class*="rating"]',
        '[class*="review"]',
    ]
    for selector in rating_selectors:
        rating_elem = soup.select_one(selector)
        if rating_elem:
            rating_text = rating_elem.get_text(strip=True)
            # Tìm số thập phân trong text
            rating_match = re.search(r"(\d+\.?\d*)", rating_text)
            if rating_match:
                try:
                    product_data["rating"]["average"] = float(rating_match.group(1))
                except Exception:
                    pass
            break

    # Tìm số lượng đánh giá
    review_count_selectors = [
        '[data-view-id="pdp_review_count"]',
        '[class*="review-count"]',
        '[class*="rating-count"]',
    ]
    for selector in review_count_selectors:
        count_elem = soup.select_one(selector)
        if count_elem:
            count_text = count_elem.get_text(strip=True)
            count_match = re.search(r"(\d+)", count_text)
            if count_match:
                try:
                    product_data["rating"]["total_reviews"] = int(count_match.group(1))
                except Exception:
                    pass
            break

    # 4. Extract seller (mở rộng selectors để lấy đầy đủ seller_name)
    seller_selectors = [
        '[data-view-id="pdp_seller_name"]',
        ".SellerName__Name-sc-",  # Tiki specific class
        '[class*="SellerName"]',
        '[class*="seller-name"]',
        '[class*="seller_info"]',
        '[data-testid="seller-name"]',
        ".seller-name",
        '[class*="seller"]',
        'a[href*="/seller/"]',  # Link to seller page
        '[href*="seller_id"]',
    ]
    seller_name_found = False
    for selector in seller_selectors:
        seller_elem = soup.select_one(selector)
        if seller_elem:
            seller_text = seller_elem.get_text(strip=True)
            # Nếu là link, lấy text hoặc title attribute
            if seller_elem.name == "a":
                seller_text = (
                    seller_elem.get_text(strip=True) or seller_elem.get("title", "").strip()
                )

            if seller_text and len(seller_text) > 1:  # Tránh lấy text quá ngắn
                product_data["seller"]["name"] = seller_text
                seller_name_found = True

                # Kiểm tra có phải official store không
                seller_text_lower = seller_text.lower()
                if (
                    "official" in seller_text_lower
                    or "chính hãng" in seller_text_lower
                    or "tiki" in seller_text_lower
                    or "tiki trading" in seller_text_lower
                ):
                    product_data["seller"]["is_official"] = True

                # Extract seller_id từ href nếu có
                if seller_elem.name == "a":
                    href = seller_elem.get("href", "")
                    seller_id_match = re.search(r"seller[_-]?id[=:](\d+)|/seller/(\d+)", href, re.I)
                    if seller_id_match:
                        product_data["seller"]["seller_id"] = seller_id_match.group(
                            1
                        ) or seller_id_match.group(2)

                break

    # Fallback: Tìm trong script tags (Tiki thường embed JSON data)
    if not seller_name_found:
        script_tags = soup.find_all("script", type="application/ld+json")
        for script in script_tags:
            try:
                data = json.loads(script.string) if script.string else {}
                # Tiki có thể lưu seller info trong structured data
                if isinstance(data, dict):
                    seller_info = (
                        data.get("brand") or data.get("seller") or data.get("manufacturer")
                    )
                    if isinstance(seller_info, dict):
                        seller_name = seller_info.get("name") or seller_info.get("@name")
                        if seller_name:
                            product_data["seller"]["name"] = seller_name
                            seller_name_found = True
                            break
            except (json.JSONDecodeError, AttributeError):
                continue

    # 5. Extract mô tả
    description_selectors = [
        '[data-view-id="pdp_product_description"]',
        ".product-description",
        '[class*="description"]',
    ]
    for selector in description_selectors:
        desc_elem = soup.select_one(selector)
        if desc_elem:
            product_data["description"] = desc_elem.get_text(strip=True)
            break

    # 6. Extract thông số kỹ thuật
    spec_section = soup.select_one(
        '[data-view-id="pdp_product_specifications"], .product-specifications, [class*="specification"]'
    )
    if spec_section:
        spec_items = spec_section.select('tr, .spec-item, [class*="spec"]')
        for item in spec_items:
            key_elem = item.select_one('td:first-child, .spec-key, [class*="key"]')
            value_elem = item.select_one('td:last-child, .spec-value, [class*="value"]')
            if key_elem and value_elem:
                key = key_elem.get_text(strip=True)
                value = value_elem.get_text(strip=True)
                if key and value:
                    product_data["specifications"][key] = value

    # 7. Extract hình ảnh
    image_selectors = [
        '[data-view-id="pdp_product_images"] img',
        ".product-images img",
        '[class*="product-image"] img',
    ]
    for selector in image_selectors:
        images = soup.select(selector)
        if images:
            for img in images[:10]:  # Lấy tối đa 10 hình
                img_url = img.get("src") or img.get("data-src") or img.get("data-lazy-src", "")
                if img_url:
                    if img_url.startswith("//"):
                        img_url = "https:" + img_url
                    elif img_url.startswith("/"):
                        img_url = "https://tiki.vn" + img_url
                    if img_url not in product_data["images"]:
                        product_data["images"].append(img_url)
            break

    # 8. Extract category path (only from breadcrumb LINKS, not product name text)
    # IMPORTANT: Tiki breadcrumbs may include product name at the end, so we limit to first 3-4 levels
    breadcrumb_selectors = [
        '[data-view-id="pdp_breadcrumb"] a',
        ".breadcrumb a",
        '[class*="breadcrumb"] a',
    ]
    MAX_CATEGORY_LEVELS = 5  # Tiki categories can have up to 5 levels with parent category included
    for selector in breadcrumb_selectors:
        breadcrumbs = soup.select(selector)
        if breadcrumbs:
            for breadcrumb in breadcrumbs:
                # Skip if we already have max levels (to avoid product name at end)
                if len(product_data["category_path"]) >= MAX_CATEGORY_LEVELS:
                    break

                text = breadcrumb.get_text(strip=True)
                href = breadcrumb.get("href", "").strip()

                # Only add if it has an href AND not a homepage link
                if text and href and text not in ["Trang chủ", "Home"]:
                    product_data["category_path"].append(text)
            break

    # 9. Extract brand
    brand_selectors = ['[data-view-id="pdp_product_brand"]', ".product-brand", '[class*="brand"]']
    for selector in brand_selectors:
        brand_elem = soup.select_one(selector)
        if brand_elem:
            product_data["brand"] = brand_elem.get_text(strip=True)
            break

    # 10. Extract thông tin vận chuyển
    shipping_text = soup.get_text()
    if "freeship" in shipping_text.lower() or "miễn phí vận chuyển" in shipping_text.lower():
        product_data["shipping"]["free_shipping"] = True
    if "giao nhanh" in shipping_text.lower() or "fast delivery" in shipping_text.lower():
        product_data["shipping"]["fast_delivery"] = True

    # 11. Parse từ __NEXT_DATA__ (ưu tiên cao nhất vì chứa dữ liệu đầy đủ)
    next_data_script = soup.find("script", id="__NEXT_DATA__")
    if next_data_script:
        try:
            next_data = json.loads(next_data_script.string)

            # Tìm product data trong __NEXT_DATA__
            # Path chính xác cho Tiki: props.initialState.productv2.productData.response.data
            def get_nested_value(obj, path):
                """Lấy giá trị từ nested dict theo path (ví dụ: 'a.b.c')"""
                keys = path.split(".")
                current = obj
                for key in keys:
                    if isinstance(current, dict):
                        current = current.get(key)
                    else:
                        return None
                    if current is None:
                        return None
                return current

            # Thử path chính xác của Tiki trước
            product_from_next = get_nested_value(
                next_data, "props.initialState.productv2.productData.response.data"
            )

            # Nếu không tìm thấy, thử các path khác
            if not product_from_next:

                def find_product_in_dict(obj, path=""):
                    if isinstance(obj, dict):
                        # Kiểm tra các key có thể chứa product data
                        if "product" in obj and isinstance(obj["product"], dict):
                            return obj["product"]
                        if "currentProduct" in obj and isinstance(obj["currentProduct"], dict):
                            return obj["currentProduct"]
                        if "productDetail" in obj and isinstance(obj["productDetail"], dict):
                            return obj["productDetail"]
                        if "data" in obj:
                            result = find_product_in_dict(obj["data"], path + ".data")
                            if result:
                                return result
                        if "props" in obj:
                            result = find_product_in_dict(obj["props"], path + ".props")
                            if result:
                                return result
                        if "pageProps" in obj:
                            result = find_product_in_dict(obj["pageProps"], path + ".pageProps")
                            if result:
                                return result
                        if "initialState" in obj:
                            result = find_product_in_dict(
                                obj["initialState"], path + ".initialState"
                            )
                            if result:
                                return result
                        # Đệ quy tìm trong tất cả các key
                        for key, value in obj.items():
                            if isinstance(value, (dict, list)):
                                result = find_product_in_dict(value, path + f".{key}")
                                if result:
                                    return result
                    elif isinstance(obj, list):
                        for item in obj:
                            result = find_product_in_dict(item, path)
                            if result:
                                return result
                    return None

                product_from_next = find_product_in_dict(next_data)

            if product_from_next:
                # Cập nhật tên
                if not product_data["name"]:
                    product_data["name"] = (
                        product_from_next.get("name")
                        or product_from_next.get("title")
                        or product_from_next.get("product_name")
                        or ""
                    )

                # Cập nhật giá
                price_info = (
                    product_from_next.get("price") or product_from_next.get("pricing") or {}
                )
                if isinstance(price_info, dict):
                    if not product_data["price"]["current_price"]:
                        product_data["price"]["current_price"] = (
                            price_info.get("salePrice")
                            or price_info.get("currentPrice")
                            or price_info.get("price")
                            or price_info.get("finalPrice")
                        )
                    if not product_data["price"]["original_price"]:
                        product_data["price"]["original_price"] = (
                            price_info.get("listPrice")
                            or price_info.get("originalPrice")
                            or price_info.get("basePrice")
                        )
                elif isinstance(price_info, (int, float)):
                    if not product_data["price"]["current_price"]:
                        product_data["price"]["current_price"] = int(price_info)

                # Tính lại discount nếu có cả 2 giá
                if (
                    product_data["price"]["current_price"]
                    and product_data["price"]["original_price"]
                ):
                    discount = (
                        (
                            product_data["price"]["original_price"]
                            - product_data["price"]["current_price"]
                        )
                        / product_data["price"]["original_price"]
                    ) * 100
                    product_data["price"]["discount_percent"] = round(discount, 1)

                # Cập nhật đánh giá
                rating_info = (
                    product_from_next.get("rating") or product_from_next.get("review") or {}
                )
                if isinstance(rating_info, dict):
                    if not product_data["rating"]["average"]:
                        product_data["rating"]["average"] = (
                            rating_info.get("average")
                            or rating_info.get("score")
                            or rating_info.get("rating")
                        )
                    if not product_data["rating"]["total_reviews"]:
                        product_data["rating"]["total_reviews"] = (
                            rating_info.get("total")
                            or rating_info.get("count")
                            or rating_info.get("reviewCount")
                            or 0
                        )

                # Cập nhật hình ảnh
                if not product_data["images"]:
                    images = (
                        product_from_next.get("images")
                        or product_from_next.get("imageUrls")
                        or product_from_next.get("gallery")
                        or product_from_next.get("images_url")
                        or []
                    )
                    if isinstance(images, list):
                        for img in images:
                            if isinstance(img, dict):
                                img_url = (
                                    img.get("url")
                                    or img.get("src")
                                    or img.get("medium_url")
                                    or img.get("large_url")
                                    or img.get("base_url")
                                    or img.get("full_path")
                                )
                            elif isinstance(img, str):
                                img_url = img
                            else:
                                continue

                            if img_url:
                                if img_url.startswith("//"):
                                    img_url = "https:" + img_url
                                elif img_url.startswith("/"):
                                    img_url = "https://tiki.vn" + img_url
                                if img_url not in product_data["images"]:
                                    product_data["images"].append(img_url)

                    # Thử tìm images trong configurable_products hoặc variants
                    if not product_data["images"]:
                        configurable_products = product_from_next.get("configurable_products", [])
                        for variant in configurable_products:
                            variant_images = (
                                variant.get("images") or variant.get("images_url") or []
                            )
                            if isinstance(variant_images, list):
                                for img in variant_images:
                                    if isinstance(img, dict):
                                        img_url = (
                                            img.get("url")
                                            or img.get("base_url")
                                            or img.get("full_path")
                                        )
                                    elif isinstance(img, str):
                                        img_url = img
                                    else:
                                        continue
                                    if img_url and img_url not in product_data["images"]:
                                        product_data["images"].append(img_url)
                            if product_data["images"]:
                                break

                # Cập nhật thông số kỹ thuật
                if not product_data["specifications"]:
                    specs = (
                        product_from_next.get("specifications")
                        or product_from_next.get("attributes")
                        or product_from_next.get("technicalSpecs")
                        or product_from_next.get("spec_attributes")
                        or {}
                    )
                    if isinstance(specs, dict):
                        product_data["specifications"] = specs
                    elif isinstance(specs, list):
                        for spec in specs:
                            if isinstance(spec, dict):
                                key = (
                                    spec.get("name")
                                    or spec.get("label")
                                    or spec.get("key")
                                    or spec.get("code")
                                    or spec.get("attribute_name")
                                )
                                value = (
                                    spec.get("value")
                                    or spec.get("text")
                                    or spec.get("option_label")
                                    or spec.get("attribute_value")
                                )
                                if key and value:
                                    product_data["specifications"][key] = value

                    # Thử tìm trong configurable_products
                    if not product_data["specifications"]:
                        configurable_products = product_from_next.get("configurable_products", [])
                        if configurable_products and isinstance(configurable_products[0], dict):
                            variant_specs = (
                                configurable_products[0].get("specifications")
                                or configurable_products[0].get("attributes")
                                or {}
                            )
                            if isinstance(variant_specs, dict):
                                product_data["specifications"] = variant_specs

                # Cập nhật mô tả
                if not product_data["description"]:
                    product_data["description"] = (
                        product_from_next.get("description")
                        or product_from_next.get("shortDescription")
                        or product_from_next.get("content")
                        or ""
                    )

                # Cập nhật brand
                if not product_data["brand"] or product_data["brand"].startswith("Thương hiệu:"):
                    brand = (
                        product_from_next.get("brand")
                        or product_from_next.get("brandName")
                        or product_from_next.get("manufacturer")
                        or ""
                    )
                    if brand:
                        product_data["brand"] = brand

                # Cập nhật seller
                seller_info = (
                    product_from_next.get("current_seller")
                    or product_from_next.get("seller")
                    or product_from_next.get("vendor")
                    or {}
                )
                if isinstance(seller_info, dict):
                    if not product_data["seller"]["name"]:
                        product_data["seller"]["name"] = (
                            seller_info.get("name") or seller_info.get("sellerName") or ""
                        )
                    if not product_data["seller"]["seller_id"]:
                        # Extract ID directly or from URL if needed
                        seller_id = seller_info.get("id") or seller_info.get("sellerId")
                        if not seller_id and seller_info.get("link"):
                            # Try to extract from link if direct ID is missing
                            import re

                            link = seller_info.get("link")
                            id_match = re.search(r"seller_id=([0-9]+)", link) or re.search(
                                r"/seller/([0-9]+)", link
                            )
                            if id_match:
                                seller_id = id_match.group(1)
                        product_data["seller"]["seller_id"] = str(seller_id) if seller_id else ""

                    if seller_info.get("isOfficial") or seller_info.get("is_official"):
                        product_data["seller"]["is_official"] = True

                # Cập nhật stock
                stock_info = (
                    product_from_next.get("stock") or product_from_next.get("inventory") or {}
                )
                if isinstance(stock_info, dict):
                    product_data["stock"]["available"] = stock_info.get("available", True)
                    product_data["stock"]["quantity"] = stock_info.get("quantity", 0)
                    product_data["stock"]["stock_status"] = stock_info.get("status", "")
                elif isinstance(stock_info, (int, float)):
                    product_data["stock"]["quantity"] = int(stock_info)
                    product_data["stock"]["available"] = stock_info > 0

                # Cập nhật sales_count (số lượng đã bán) - dùng shared utility
                if not product_data["sales_count"]:
                    sales_count_raw = (
                        product_from_next.get("sales_count")
                        or product_from_next.get("quantity_sold")
                        or product_from_next.get("sold_count")
                        or product_from_next.get("total_sold")
                        or product_from_next.get("order_count")
                        or product_from_next.get("sales_quantity")
                        or product_from_next.get("quantity")
                        or product_from_next.get("sold")
                        or product_from_next.get("total_quantity_sold")
                    )
                    product_data["sales_count"] = parse_sales_count(sales_count_raw)
        except Exception as e:
            if verbose:
                print(f"[Parse] Lỗi khi parse __NEXT_DATA__: {e}")
            pass

    # Fix: Ensure category_path is never None, use empty list if not populated
    if product_data.get("category_path") is None:
        product_data["category_path"] = []

    # Ensure category_path is always a list
    if not isinstance(product_data.get("category_path"), list):
        product_data["category_path"] = [str(product_data["category_path"])]

    # Enforce max category levels (safety check against product names)
    # Real Tiki categories should have 3-5 levels (including parent), anything more is likely product name
    MAX_CATEGORY_LEVELS = 5
    if len(product_data["category_path"]) > MAX_CATEGORY_LEVELS:
        product_data["category_path"] = product_data["category_path"][:MAX_CATEGORY_LEVELS]

    # Remove product name from category_path if it was mistakenly added
    # (sometimes breadcrumbs or API data include product name at the end)
    product_name = product_data.get("name", "").strip()
    if product_name and product_data["category_path"]:
        # Check both exact match and prefix match (product name may be truncated in category_path)
        filtered_path = []
        for cat in product_data["category_path"]:
            # Skip if category text is suspiciously long (>80 chars = likely product name)
            # OR if it starts with the product name
            if len(cat) > 80 or (
                product_name and cat.upper().startswith(product_name[:30].upper())
            ):
                continue
            filtered_path.append(cat)

        # If we filtered out the last item and product_name was in path exactly, use filtered version
        if product_name in product_data["category_path"]:
            filtered_path = [c for c in product_data["category_path"] if c != product_name]

        product_data["category_path"] = filtered_path

    # Prepend parent_category to category_path if provided
    if parent_category and isinstance(parent_category, str) and parent_category.strip():
        try:
            # Remove parent_category from path if it's already there (avoid duplicates)
            existing_path = [c for c in product_data["category_path"] if c != parent_category]
            # Prepend parent_category to the beginning
            product_data["category_path"] = [parent_category] + existing_path

            # Ensure we don't exceed max levels - if we have too many levels, trim from the end
            if len(product_data["category_path"]) > 5:
                product_data["category_path"] = product_data["category_path"][:5]
        except Exception as e:
            # Log but don't fail - just keep original path
            if verbose:
                print(f"[Warning] Error prepending parent category '{parent_category}': {e}")
    elif (
        not parent_category
        and len(product_data["category_path"]) >= 3
        and len(product_data["category_path"]) <= 4
    ):
        # Auto-detect parent category from hierarchy if path has 3-4 levels
        # This handles the case where breadcrumb is missing Level 0 (or has 4 levels without parent)
        try:
            if hierarchy_map is None:
                hierarchy_map = load_category_hierarchy()

            if hierarchy_map:
                # Try to find parent from first category item's hierarchy
                first_item = (
                    product_data["category_path"][0] if product_data["category_path"] else None
                )

                if first_item:
                    try:
                        # Build reverse lookup: name -> url for faster search
                        name_to_url = {info.get("name"): url for url, info in hierarchy_map.items()}

                        if first_item in name_to_url:
                            # Found this category in hierarchy, get its root parent
                            cat_url = name_to_url[first_item]
                            parent_name = get_parent_category_name(cat_url, hierarchy_map)
                            if parent_name and parent_name != first_item:
                                # Check if parent is already at the beginning (avoid duplicates)
                                if product_data["category_path"][0] != parent_name:
                                    # Add parent to beginning
                                    product_data["category_path"] = [parent_name] + product_data[
                                        "category_path"
                                    ]
                                    # Ensure we don't exceed max levels - if we have too many levels, trim from the end
                                    if len(product_data["category_path"]) > 5:
                                        product_data["category_path"] = product_data[
                                            "category_path"
                                        ][:5]
                    except Exception as e:
                        # Log but don't fail - just keep original path
                        if verbose:
                            print(f"[Warning] Error auto-detecting parent for '{first_item}': {e}")
        except Exception as e:
            # Log but don't fail - just keep original path
            if verbose:
                print(f"[Warning] Error loading hierarchy map for auto-detect: {e}")

    # Final validation: ensure category_path is valid and has parent category at index 0
    try:
        from .validate_category_path import validate_and_fix_category_path

        category_url = product_data.get("category_url")
        category_path = product_data.get("category_path")

        # Validate và fix category_path để đảm bảo parent category ở index 0
        product_data["category_path"] = validate_and_fix_category_path(
            category_path=category_path,
            category_url=category_url,
            hierarchy_map=hierarchy_map,
            auto_fix=True,
        )

    except Exception as e:
        if verbose:
            print(f"[Warning] Error validating category_path: {e}")
        # Fallback: Đảm bảo ít nhất là một list hợp lệ
        try:
            if not isinstance(product_data.get("category_path"), list):
                product_data["category_path"] = []
            else:
                # Basic cleanup
                product_data["category_path"] = [
                    str(item).strip()
                    for item in product_data["category_path"]
                    if item and isinstance(item, (str, int, float))
                ]
        except Exception:
            product_data["category_path"] = []

    return product_data


async def crawl_product_detail_async(
    url: str,
    session=None,
    use_selenium_fallback: bool = True,
    verbose: bool = False,
) -> dict[str, Any] | None:
    """Crawl product detail bằng aiohttp (async) với Selenium fallback

    Hybrid approach:
    - Thử crawl bằng aiohttp trước (nhanh hơn)
    - Nếu thiếu dynamic content (sales_count), fallback về Selenium

    Args:
        url: URL sản phẩm cần crawl
        session: aiohttp ClientSession (nếu None sẽ tạo mới)
        use_selenium_fallback: Có dùng Selenium fallback không
        verbose: Có in log không

    Returns:
        Dict product detail hoặc None nếu lỗi
    """
    try:
        import aiohttp
    except ImportError:
        if verbose:
            print("[Async] ⚠️  aiohttp chưa được cài đặt, dùng Selenium fallback")
        if use_selenium_fallback:
            return crawl_product_detail_with_selenium(url, verbose=verbose)
        return None

    # Tạo session nếu chưa có
    create_session = session is None
    if create_session:
        import aiohttp

        # Tối ưu: TCPConnector với pool size lớn hơn (sử dụng config)
        from .config import (
            HTTP_CONNECTOR_LIMIT,
            HTTP_CONNECTOR_LIMIT_PER_HOST,
            HTTP_DNS_CACHE_TTL,
        )

        connector = aiohttp.TCPConnector(
            limit=HTTP_CONNECTOR_LIMIT,  # Sử dụng config (150)
            limit_per_host=HTTP_CONNECTOR_LIMIT_PER_HOST,  # Sử dụng config (15)
            ttl_dns_cache=HTTP_DNS_CACHE_TTL,  # Sử dụng config (1800s = 30 min)
            ssl=False,  # Disable SSL verification cho tốc độ
            force_close=False,  # Keep connections alive for reuse
            enable_cleanup_closed=True,
        )
        timeout = aiohttp.ClientTimeout(total=20, connect=10)  # Tối ưu: giảm timeout
        session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            },
        )

    try:
        # Thử crawl bằng aiohttp
        async with session.get(url) as response:
            if response.status != 200:
                if verbose:
                    print(f"[Async] ⚠️  HTTP {response.status} cho {url[:60]}...")
                if use_selenium_fallback:
                    if create_session:
                        await session.close()
                    return crawl_product_detail_with_selenium(url, verbose=verbose)
                return None

            html_content = await response.text()

            # Extract product detail từ HTML
            product_data = extract_product_detail(html_content, url, verbose=verbose)

            # Kiểm tra xem có đầy đủ thông tin không (đặc biệt là sales_count)
            has_sales_count = product_data.get("sales_count") is not None

            # Nếu thiếu sales_count và có fallback, dùng Selenium
            if not has_sales_count and use_selenium_fallback:
                if verbose:
                    print(f"[Async] ⚠️  Thiếu sales_count, fallback về Selenium cho {url[:60]}...")
                if create_session:
                    await session.close()
                return crawl_product_detail_with_selenium(url, verbose=verbose)

            # Cập nhật metadata
            product_data["_metadata"]["extraction_method"] = (
                "aiohttp" if has_sales_count else "aiohttp+selenium_fallback"
            )

            if verbose:
                print(f"[Async] ✅ Crawl thành công: {product_data.get('name', 'Unknown')[:50]}...")

            # Fix: Ensure category_path is never None, use empty list if not populated
            if product_data.get("category_path") is None:
                product_data["category_path"] = []

            # Ensure category_path is always a list
            if not isinstance(product_data.get("category_path"), list):
                product_data["category_path"] = [str(product_data["category_path"])]

            # Enforce max category levels (safety check against product names)
            # Real Tiki categories should have 3-5 levels (including parent), anything more is likely product name
            MAX_CATEGORY_LEVELS = 5
            if len(product_data["category_path"]) > MAX_CATEGORY_LEVELS:
                product_data["category_path"] = product_data["category_path"][:MAX_CATEGORY_LEVELS]

            # Remove product name from category_path if it was mistakenly added
            product_name = product_data.get("name", "").strip()
            if product_name and product_data["category_path"]:
                # Check both exact match and prefix match (product name may be truncated in category_path)
                filtered_path = []
                for cat in product_data["category_path"]:
                    # Skip if category text is suspiciously long (>80 chars = likely product name)
                    # OR if it starts with the product name
                    if len(cat) > 80 or (
                        product_name and cat.upper().startswith(product_name[:30].upper())
                    ):
                        continue
                    filtered_path.append(cat)

                # If we filtered out the last item and product_name was in path exactly, use filtered version
                if product_name in product_data["category_path"]:
                    filtered_path = [c for c in product_data["category_path"] if c != product_name]

                product_data["category_path"] = filtered_path

            return product_data

    except Exception as e:
        if verbose:
            print(f"[Async] ❌ Lỗi khi crawl với aiohttp: {e}")
        # Fallback về Selenium nếu có lỗi
        if use_selenium_fallback:
            if create_session:
                await session.close()
            return crawl_product_detail_with_selenium(url, verbose=verbose)
        return None
    finally:
        if create_session and session:
            await session.close()


def main():
    """Test crawl sản phẩm"""
    url = "https://tiki.vn/binh-giu-nhiet-inox-304-elmich-el-8013ol-dung-tich-480ml-p120552065.html?itm_campaign=CTP_YPD_TKA_PLA_UNK_ALL_UNK_UNK_UNK_UNK_X.304582_Y.1886902_Z.4008723_CN.HL-l-Binh-Giu-Nhiet&itm_medium=CPC&itm_source=tiki-ads&spid=120552067"

    print("=" * 70)
    print("CRAWL SẢN PHẨM TIKI VỚI SELENIUM")
    print("=" * 70)
    print(f"URL: {url}\n")

    # Bước 1: Crawl với Selenium
    print("BƯỚC 1: Crawl với Selenium")
    print("-" * 70)
    html_content = crawl_product_detail_with_selenium(url, save_html=True, verbose=True)
    print(f"✓ Đã lấy HTML ({len(html_content)} ký tự)\n")

    # Bước 2: Extract thông tin sản phẩm
    print("BƯỚC 2: Extract thông tin sản phẩm")
    print("-" * 70)
    product_data = extract_product_detail(html_content, url, verbose=True)

    # In kết quả
    print(f"Product ID: {product_data['product_id']}")
    print(f"Tên: {product_data['name']}")
    print(
        f"Giá hiện tại: {product_data['price']['current_price']:,} {product_data['price']['currency']}"
        if product_data["price"]["current_price"]
        else "Giá: N/A"
    )
    print(
        f"Giá gốc: {product_data['price']['original_price']:,} {product_data['price']['currency']}"
        if product_data["price"]["original_price"]
        else "Giá gốc: N/A"
    )
    print(
        f"Giảm giá: {product_data['price']['discount_percent']}%"
        if product_data["price"]["discount_percent"]
        else "Giảm giá: N/A"
    )
    print(
        f"Đánh giá: {product_data['rating']['average']}/5 ({product_data['rating']['total_reviews']} đánh giá)"
        if product_data["rating"]["average"]
        else "Đánh giá: N/A"
    )
    print(
        f"Seller: {product_data['seller']['name']}"
        if product_data["seller"]["name"]
        else "Seller: N/A"
    )
    print(f"Brand: {product_data['brand']}" if product_data["brand"] else "Brand: N/A")
    print(f"Số hình ảnh: {len(product_data['images'])}")
    print(f"Số thông số: {len(product_data['specifications'])}")
    print(
        f"Category path: {' > '.join(product_data['category_path'])}"
        if product_data["category_path"]
        else "Category path: N/A"
    )

    # Lưu kết quả JSON
    output_dir = "data/test_output"
    import os

    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, "selenium_product_detail.json")
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(product_data, f, ensure_ascii=False, indent=2)

    print(f"\n✓ Đã lưu kết quả vào {output_file}")
    print("\n" + "=" * 70)
    print("✓ Hoàn thành!")
    print("=" * 70)


if __name__ == "__main__":
    main()
