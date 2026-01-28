import json
import re
import sys
import time

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

# Thử import webdriver-manager
try:
    from webdriver_manager.chrome import ChromeDriverManager

    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

# Set UTF-8 encoding cho stdout trên Windows
if sys.platform == "win32":
    import io

    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")


# ========== SELENIUM ==========
def crawl_with_selenium(url, save_html=False, verbose=True):
    """Crawl trang bằng Selenium để load đầy đủ dữ liệu

    Args:
        url: URL cần crawl
        save_html: Có lưu HTML vào file không (mặc định False để tối ưu)
        verbose: Có in log không (mặc định True)
    """

    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    )

    # Tối ưu: tắt một số tính năng không cần
    prefs = {
        "profile.managed_default_content_settings.images": 2,  # Tắt hình ảnh
        "profile.default_content_setting_values.notifications": 2,
    }
    chrome_options.add_experimental_option("prefs", prefs)

    # Eager page load for faster loading
    try:
        chrome_options.page_load_strategy = "eager"
    except Exception:
        pass

    # Sử dụng webdriver-manager nếu có
    if HAS_WEBDRIVER_MANAGER:
        try:
            import os
            import stat

            # Install ChromeDriver
            driver_path = ChromeDriverManager().install()

            # QUAN TRỌNG: Set quyền thực thi cho ChromeDriver (fix lỗi status code 127)
            # Đặc biệt cần thiết trong WSL2/Linux
            try:
                os.chmod(
                    driver_path,
                    os.stat(driver_path).st_mode | stat.S_IEXEC,
                )
            except Exception:
                pass  # Nếu không set được quyền, vẫn thử tiếp

            # Kiểm tra ChromeDriver có thể chạy được không
            try:
                import subprocess

                result = subprocess.run(
                    [driver_path, "--version"], capture_output=True, timeout=5, text=True
                )
                if result.returncode != 0:
                    raise RuntimeError(
                        "ChromeDriver không thể chạy được. "
                        "Có thể thiếu dependencies (libnss3, libnspr4, etc.). "
                        "Chạy: bash scripts/install_chromedriver_dependencies.sh"
                    )
            except Exception:
                pass  # Nếu kiểm tra fail, vẫn thử tiếp

            service = Service(driver_path)
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            error_msg = str(e)
            if "127" in error_msg or "unexpectedly exited" in error_msg.lower():
                enhanced_error = RuntimeError(
                    f"ChromeDriver không thể khởi động (status code 127). "
                    f"Thiếu dependencies hệ thống (libnss3, libnspr4, etc.).\n"
                    f"Giải pháp: Chạy script sau để cài đặt dependencies:\n"
                    f"  bash scripts/install_chromedriver_dependencies.sh\n"
                    f"Lỗi gốc: {error_msg}"
                )
                if verbose:
                    print(f"[Selenium] {enhanced_error}")
                raise enhanced_error from e

            if verbose:
                print(f"[Selenium] Không thể dùng webdriver-manager: {e}, thử Chrome mặc định")
            try:
                driver = webdriver.Chrome(options=chrome_options)
            except Exception as e2:
                if verbose:
                    print(f"[Selenium] Lỗi khi tạo Chrome driver: {e2}")
                raise
    else:
        driver = webdriver.Chrome(options=chrome_options)

    try:
        if verbose:
            print(f"[Selenium] Đang mở {url}...")
        driver.get(url)

        # Optimized wait time
        time.sleep(0.5)  # Reduced from 2s to 0.5s

        # Scroll để load phần "Khám phá theo danh mục"
        if verbose:
            print("[Selenium] Đang scroll để load phần 'Khám phá theo danh mục'...")
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(0.3)  # Reduced from 0.5s to 0.3s
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(0.3)  # Reduced from 0.5s to 0.3s

        # Tìm và click các nút expand danh mục (nếu có)
        try:
            expand_buttons = driver.find_elements(
                By.CSS_SELECTOR, ".arrow-icon, [class*='arrow'], [class*='expand']"
            )
            for btn in expand_buttons[:5]:  # Limit to 5 buttons
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                    time.sleep(0.15)  # Reduced from 0.2s to 0.15s
                    btn.click()
                    time.sleep(0.2)  # Reduced from 0.3s to 0.2s
                    if verbose:
                        print("[Selenium] Đã click expand danh mục")
                except Exception:
                    continue
        except Exception:
            pass

        # Scroll thêm để đảm bảo load hết
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)  # Reduced from 1s to 0.5s

        # Lấy HTML đầy đủ
        full_html = driver.page_source

        # Chỉ lưu HTML nếu được yêu cầu
        if save_html:
            with open("full_page.html", "w", encoding="utf-8") as f:
                f.write(full_html)
            if verbose:
                print("[Selenium] ✓ Đã lưu HTML vào full_page.html")

        return full_html

    finally:
        driver.quit()


def crawl_with_driver(driver, url, save_html=False, verbose=True):
    """Crawl trang bằng Selenium driver đã được cấp sẵn (driver reuse/pooling)

    Args:
        driver: Selenium WebDriver đã được tạo sẵn
        url: URL cần crawl
        save_html: Có lưu HTML vào file không
        verbose: Có in log không

    Returns:
        str: HTML content hoặc None nếu lỗi
    """
    try:
        if verbose:
            print(f"[Selenium] Đang mở {url}... (reuse driver)")
        driver.get(url)

        # Optimized wait time
        time.sleep(0.5)

        # Scroll để load phần "Khám phá theo danh mục"
        if verbose:
            print("[Selenium] Đang scroll để load phần 'Khám phá theo danh mục'...")
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(0.3)
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(0.3)

        # Tìm và click các nút expand danh mục (nếu có)
        try:
            expand_buttons = driver.find_elements(
                By.CSS_SELECTOR, ".arrow-icon, [class*='arrow'], [class*='expand']"
            )
            for btn in expand_buttons[:5]:
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                    time.sleep(0.15)
                    btn.click()
                    time.sleep(0.2)
                    if verbose:
                        print("[Selenium] Đã click expand danh mục")
                except Exception:
                    continue
        except Exception:
            pass

        # Scroll thêm để đảm bảo load hết
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(0.5)

        # Lấy HTML đầy đủ
        full_html = driver.page_source

        # Chỉ lưu HTML nếu được yêu cầu
        if save_html:
            with open("full_page.html", "w", encoding="utf-8") as f:
                f.write(full_html)
            if verbose:
                print("[Selenium] ✓ Đã lưu HTML vào full_page.html")

        return full_html

    except Exception as e:
        if verbose:
            print(f"[Selenium] Lỗi khi crawl (reuse driver): {type(e).__name__}: {e}")
        return None


# ========== PARSE DỮ LIỆU ==========
def parse_categories(html_content, parent_url=None, level=0):
    """Parse các link danh mục từ phần 'Khám phá theo danh mục' của Tiki"""

    soup = BeautifulSoup(html_content, "html.parser")
    categories = []
    seen_urls = set()  # Để tránh trùng lặp trong cùng một trang

    # Pattern URL danh mục Tiki: /c[0-9]+
    category_pattern = re.compile(r"/c\d+")

    # Tìm tất cả các phần "Khám phá theo danh mục"
    # Có thể có nhiều phần, chúng ta sẽ chọn phần tốt nhất (nhiều danh mục có hình ảnh nhất)
    candidate_sections = []

    # 1. Tìm theo text "Khám phá theo danh mục"
    for elem in soup.find_all(string=re.compile(r"Khám phá theo danh mục", re.I)):
        parent = elem.find_parent()
        if parent:
            section = parent.find_parent()
            if section:
                candidate_sections.append(section)

    # 2. Bổ sung các section theo selectors nếu ít candidate quá
    selectors = [
        {"class": re.compile(r"category", re.I)},
        {"class": re.compile(r"explore", re.I)},
        {"data-view-id": re.compile(r"category", re.I)},
    ]
    for selector in selectors:
        sections = soup.find_all("div", selector)
        candidate_sections.extend(sections)

    if not candidate_sections:
        print("[Parse] ⚠ Không tìm thấy phần 'Khám phá theo danh mục' nào")
        return []

    print(f"[Parse] Đang kiểm tra {len(candidate_sections)} candidate sections...")

    best_categories_with_images = []
    best_section_index = -1

    for i, section in enumerate(candidate_sections):
        current_section_categories = []
        current_seen_urls = set()

        # Tìm tất cả link có pattern danh mục trong section này
        all_links = section.find_all("a", href=True)
        
        for link in all_links:
            href = link.get("href", "")
            if not href or not category_pattern.search(href):
                continue

            # Chuẩn hóa URL
            if href.startswith("//"): href = "https:" + href
            elif href.startswith("/"): href = "https://tiki.vn" + href
            elif not href.startswith("http"): continue

            if "?" in href:
                base_url = href.split("?")[0]
                if category_pattern.search(base_url):
                    href = base_url

            if href in current_seen_urls:
                continue
            current_seen_urls.add(href)

            try:
                # Tìm hình ảnh
                img = link.find("img")
                if not img:
                    container = link.find_parent(["div", "li", "article", "section"])
                    if container:
                        img = container.find("img")
                if not img:
                    container = link.find_parent(["div", "li", "article", "section"])
                    if container:
                        imgs = container.find_all("img")
                        if imgs: img = imgs[0]

                img_url = ""
                if img:
                    img_url = (img.get("src", "") or img.get("data-src", "") or img.get("data-lazy-src", ""))
                    if img_url:
                        if img_url.startswith("//"): img_url = "https:" + img_url
                        elif img_url.startswith("/"): img_url = "https://tiki.vn" + img_url

                # Mandatory: Bắt buộc có image_url
                if not img_url or not img_url.strip():
                    continue

                # Lấy tên
                name = link.get_text(strip=True)
                if not name and img:
                    name = img.get("alt", "").strip()
                if not name:
                    name_elem = link.find(["span", "div", "h3", "h4"])
                    if name_elem: name = name_elem.get_text(strip=True)
                if not name:
                    url_parts = href.split("/")
                    if len(url_parts) >= 4:
                        name = url_parts[-2].replace("-", " ").title()

                if name and len(name.strip()) > 1:
                    slug = ""
                    url_parts = href.split("/")
                    if len(url_parts) >= 4: slug = url_parts[-2]

                    current_section_categories.append({
                        "name": name.strip(),
                        "slug": slug,
                        "url": href,
                        "image_url": img_url,
                        "parent_url": parent_url,
                        "level": level,
                    })
            except Exception:
                continue

        # Kiểm tra xem đây có phải section tốt nhất không
        if len(current_section_categories) > len(best_categories_with_images):
            best_categories_with_images = current_section_categories
            best_section_index = i

    if not best_categories_with_images:
        print("[Parse] ⚠ Không tìm thấy danh mục nào có hình ảnh trong tất cả sections")
        return []

    print(f"[Parse] ✓ Chọn Section {best_section_index} với {len(best_categories_with_images)} danh mục có hình ảnh")


    # Lưu dữ liệu JSON (chỉ khi không phải đệ quy)
    if level == 0 and parent_url is None:
        output_file = "data/raw/categories.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(best_categories_with_images, f, ensure_ascii=False, indent=2)
        print(f"[Parse] ✓ Đã lưu dữ liệu vào {output_file}")

    return best_categories_with_images



# ========== MAIN ==========
if __name__ == "__main__":
    url = "https://tiki.vn/nha-cua-doi-song/c1883"

    # Bước 1: Crawl với Selenium
    print("=" * 50)
    print("BƯỚC 1: Crawl với Selenium")
    print("=" * 50)
    html_content = crawl_with_selenium(url)

    # Bước 2: Parse dữ liệu danh mục
    print("\n" + "=" * 50)
    print("BƯỚC 2: Parse dữ liệu danh mục")
    print("=" * 50)
    categories = parse_categories(html_content)

    # In vài danh mục đầu
    print("\nVài danh mục đầu tiên:")
    for category in categories[:10]:
        print(f"  - {category['name']}: {category['url']}")

    print("\n" + "=" * 50)
    print("✓ Hoàn thành!")
    print("=" * 50)
    print("Các file output:")
    print("  - full_page.html (HTML đầy đủ)")
    print("  - categories.json (Dữ liệu danh mục)")
