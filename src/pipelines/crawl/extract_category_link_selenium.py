from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import time
import json
import re
import sys
from bs4 import BeautifulSoup

# Thử import webdriver-manager
try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WEBDRIVER_MANAGER = True
except ImportError:
    HAS_WEBDRIVER_MANAGER = False

# Set UTF-8 encoding cho stdout trên Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

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
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    
    # Tối ưu: tắt một số tính năng không cần
    prefs = {
        "profile.managed_default_content_settings.images": 2,  # Tắt hình ảnh
        "profile.default_content_setting_values.notifications": 2
    }
    chrome_options.add_experimental_option("prefs", prefs)
    
    # Sử dụng webdriver-manager nếu có
    if HAS_WEBDRIVER_MANAGER:
        try:
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            if verbose:
                print(f"[Selenium] Không thể dùng webdriver-manager: {e}, thử Chrome mặc định")
            driver = webdriver.Chrome(options=chrome_options)
    else:
        driver = webdriver.Chrome(options=chrome_options)
    
    try:
        if verbose:
            print(f"[Selenium] Đang mở {url}...")
        driver.get(url)
        
        # Giảm thời gian chờ
        time.sleep(2)  # Giảm từ 3 xuống 2
        
        # Scroll để load phần "Khám phá theo danh mục"
        if verbose:
            print("[Selenium] Đang scroll để load phần 'Khám phá theo danh mục'...")
        driver.execute_script("window.scrollTo(0, 500);")
        time.sleep(0.5)  # Giảm từ 1 xuống 0.5
        driver.execute_script("window.scrollTo(0, 1000);")
        time.sleep(0.5)
        
        # Tìm và click các nút expand danh mục (nếu có)
        try:
            expand_buttons = driver.find_elements(By.CSS_SELECTOR, ".arrow-icon, [class*='arrow'], [class*='expand']")
            for btn in expand_buttons[:5]:  # Giảm từ 10 xuống 5
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                    time.sleep(0.2)  # Giảm từ 0.3 xuống 0.2
                    btn.click()
                    time.sleep(0.3)  # Giảm từ 0.5 xuống 0.3
                    if verbose:
                        print(f"[Selenium] Đã click expand danh mục")
                except:
                    continue
        except:
            pass
        
        # Scroll thêm để đảm bảo load hết
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)  # Giảm từ 2 xuống 1
        
        # Lấy HTML đầy đủ
        full_html = driver.page_source
        
        # Chỉ lưu HTML nếu được yêu cầu
        if save_html:
            with open('full_page.html', 'w', encoding='utf-8') as f:
                f.write(full_html)
            if verbose:
                print("[Selenium] ✓ Đã lưu HTML vào full_page.html")
        
        return full_html
        
    finally:
        driver.quit()


# ========== PARSE DỮ LIỆU ==========
def parse_categories(html_content, parent_url=None, level=0):
    """Parse các link danh mục từ phần 'Khám phá theo danh mục' của Tiki"""
    
    soup = BeautifulSoup(html_content, 'html.parser')
    categories = []
    seen_urls = set()  # Để tránh trùng lặp trong cùng một trang
    
    # Pattern URL danh mục Tiki: /c[0-9]+
    category_pattern = re.compile(r'/c\d+')
    
    # Tìm tất cả các phần "Khám phá theo danh mục"
    # Và chỉ lấy phần có hình ảnh
    category_sections = []
    
    # Tìm theo text "Khám phá theo danh mục"
    for elem in soup.find_all(string=re.compile(r'Khám phá theo danh mục', re.I)):
        parent = elem.find_parent()
        if parent:
            # Tìm container chứa danh mục (thường là div cha)
            section = parent.find_parent()
            if section:
                category_sections.append(section)
    
    print(f"[Parse] Tìm thấy {len(category_sections)} phần 'Khám phá theo danh mục'")
    
    # Tìm phần có hình ảnh
    category_section = None
    for section in category_sections:
        # Kiểm tra xem section này có chứa hình ảnh không
        images = section.find_all('img')
        if images:
            category_section = section
            print(f"[Parse] ✓ Tìm thấy phần có hình ảnh ({len(images)} hình)")
            break
    
    # Nếu không tìm thấy bằng text, thử tìm theo class/attribute và kiểm tra hình ảnh
    if not category_section:
        selectors = [
            {'class': re.compile(r'category', re.I)},
            {'class': re.compile(r'explore', re.I)},
            {'data-view-id': re.compile(r'category', re.I)},
        ]
        for selector in selectors:
            sections = soup.find_all('div', selector)
            for section in sections:
                images = section.find_all('img')
                if images:
                    category_section = section
                    print(f"[Parse] ✓ Tìm thấy phần có hình ảnh với selector: {selector} ({len(images)} hình)")
                    break
            if category_section:
                break
    
    # Nếu không tìm thấy section có hình ảnh, báo lỗi
    if not category_section:
        print(f"[Parse] ⚠ Không tìm thấy phần 'Khám phá theo danh mục' có hình ảnh")
        return []
    
    # Tìm tất cả link có pattern danh mục trong section
    all_links = category_section.find_all('a', href=True)
    print(f"[Parse] Tìm thấy {len(all_links)} link trong section")
    
    # Lọc các link danh mục
    category_links = []
    for link in all_links:
        href = link.get('href', '')
        if not href:
            continue
        
        # Kiểm tra xem có phải link danh mục không (chứa /c[0-9]+)
        if not category_pattern.search(href):
            continue
        
        # Chuẩn hóa URL
        if href.startswith('//'):
            href = 'https:' + href
        elif href.startswith('/'):
            href = 'https://tiki.vn' + href
        elif not href.startswith('http'):
            continue
        
        # Bỏ qua các URL có query params không cần thiết
        if '?' in href:
            base_url = href.split('?')[0]
            if category_pattern.search(base_url):
                href = base_url
        
        # Bỏ qua nếu đã có trong danh sách
        if href in seen_urls:
            continue
        seen_urls.add(href)
        
        category_links.append((link, href))
    
    print(f"[Parse] Tìm thấy {len(category_links)} link danh mục hợp lệ")
    
    # Với mỗi link danh mục, tìm hình ảnh và thông tin liên quan
    for link, href in category_links:
        try:
            # Tìm hình ảnh liên quan đến link này
            # Cách 1: Hình ảnh nằm trong link
            img = link.find('img')
            
            # Cách 2: Hình ảnh nằm trong container cùng với link
            if not img:
                container = link.find_parent(['div', 'li', 'article', 'section'])
                if container:
                    img = container.find('img')
            
            # Cách 3: Tìm hình ảnh gần nhất trong cùng container
            if not img:
                container = link.find_parent(['div', 'li', 'article', 'section'])
                if container:
                    # Tìm tất cả hình ảnh trong container và lấy hình đầu tiên
                    imgs = container.find_all('img')
                    if imgs:
                        img = imgs[0]
            
            # Lấy URL hình ảnh
            img_url = ''
            if img:
                img_url = img.get('src', '') or img.get('data-src', '') or img.get('data-lazy-src', '')
                if img_url:
                    # Chuẩn hóa URL hình ảnh
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                    elif img_url.startswith('/'):
                        img_url = 'https://tiki.vn' + img_url
            
            # Lấy tên danh mục từ text của link hoặc alt của hình ảnh
            name = link.get_text(strip=True)
            
            # Nếu không có text, thử lấy từ alt của hình ảnh
            if not name and img:
                name = img.get('alt', '').strip()
            
            # Nếu vẫn không có, thử tìm trong các thẻ con của link
            if not name:
                name_elem = (
                    link.find('span', class_=lambda x: x and ('title' in str(x).lower() or 'name' in str(x).lower())) or
                    link.find('div', class_=lambda x: x and ('title' in str(x).lower() or 'name' in str(x).lower())) or
                    link.find('h3') or
                    link.find('h4') or
                    link.find('span') or
                    link.find('div')
                )
                if name_elem:
                    name = name_elem.get_text(strip=True)
            
            # Nếu vẫn không có tên, lấy từ URL
            if not name:
                url_parts = href.split('/')
                if len(url_parts) >= 4:
                    name = url_parts[-2].replace('-', ' ').title()
            
            # Extract slug từ URL
            slug = ''
            url_parts = href.split('/')
            if len(url_parts) >= 4:
                slug = url_parts[-2]
            
            # Chỉ thêm nếu có tên hợp lệ
            if name and len(name.strip()) > 1:
                category_data = {
                    'name': name.strip(),
                    'slug': slug,
                    'url': href,
                    'image_url': img_url if img_url else '',
                    'parent_url': parent_url,
                    'level': level
                }
                categories.append(category_data)
        except Exception as e:
            # Bỏ qua lỗi parse từng link
            print(f"[Parse] Lỗi khi parse link {href}: {e}")
            continue
    
    # Sắp xếp và loại bỏ trùng lặp theo URL
    unique_categories = []
    seen_urls_final = set()
    for cat in categories:
        if cat['url'] not in seen_urls_final:
            unique_categories.append(cat)
            seen_urls_final.add(cat['url'])
    
    # Lọc các danh mục có URL hình ảnh
    categories_with_images = []
    for cat in unique_categories:
        if cat.get('image_url', '').strip():
            categories_with_images.append(cat)
    
    print(f"[Parse] ✓ Đã tìm thấy {len(unique_categories)} danh mục (sau khi loại trùng)")
    print(f"[Parse] ✓ Đã lọc được {len(categories_with_images)} danh mục có hình ảnh")
    
    # Lưu dữ liệu JSON (chỉ khi không phải đệ quy)
    if level == 0 and parent_url is None:
        output_file = 'data/raw/categories.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(categories_with_images, f, ensure_ascii=False, indent=2)
        print(f"[Parse] ✓ Đã lưu dữ liệu vào {output_file}")
    
    return categories_with_images


# ========== MAIN ==========
if __name__ == "__main__":
    url = "https://tiki.vn/nha-cua-doi-song/c1883"
    
    # Bước 1: Crawl với Selenium
    print("="*50)
    print("BƯỚC 1: Crawl với Selenium")
    print("="*50)
    html_content = crawl_with_selenium(url)
    
    # Bước 2: Parse dữ liệu danh mục
    print("\n" + "="*50)
    print("BƯỚC 2: Parse dữ liệu danh mục")
    print("="*50)
    categories = parse_categories(html_content)
    
    # In vài danh mục đầu
    print(f"\nVài danh mục đầu tiên:")
    for category in categories[:10]:
        print(f"  - {category['name']}: {category['url']}")
    
    print("\n" + "="*50)
    print("✓ Hoàn thành!")
    print("="*50)
    print("Các file output:")
    print("  - full_page.html (HTML đầy đủ)")
    print("  - categories.json (Dữ liệu danh mục)")

