"""
Extract products và links từ Tiki categories
"""
import os
import sys
import re
import requests
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, parse_qs
from typing import List, Dict, Any, Optional

# Fix encoding on Windows
if sys.platform == "win32":
    import io
    try:
        if not hasattr(sys.stdout, 'buffer') or (hasattr(sys.stdout, 'encoding') and sys.stdout.encoding != 'utf-8'):
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass

from .config import get_config

# Cấu hình
config = get_config()
FIRECRAWL_API_URL = os.getenv("FIRECRAWL_API_URL", "http://localhost:3002")
TIKI_BASE_URL = "https://tiki.vn"


def extract_product_id(url: str) -> Optional[str]:
    """Extract product ID từ URL (pattern: /p123456 hoặc ?id=123456 hoặc -p123456.html)"""
    # Pattern 1: /p123456
    match = re.search(r'/p(\d+)', url)
    if match:
        return match.group(1)
    
    # Pattern 2: -p123456 or -p123456.html (Tiki product URL format)
    match = re.search(r'-p(\d+)', url)
    if match:
        return match.group(1)
    
    # Pattern 3: ?id=123456
    parsed = urlparse(url)
    query_params = parse_qs(parsed.query)
    if 'id' in query_params:
        return query_params['id'][0]
    
    return None


def extract_products_from_markdown(markdown_text: str, category_id: str = None, category_name: str = None) -> List[Dict[str, Any]]:
    """
    Extract product links từ markdown text
    
    Args:
        markdown_text: Markdown content từ Firecrawl
        category_id: ID của category (optional)
        category_name: Tên category (optional)
    
    Returns:
        List of products với 'name', 'url', 'product_id'
    """
    products = []
    
    # Pattern để tìm markdown links: [text](url)
    link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
    matches = re.findall(link_pattern, markdown_text)
    
    for name, url in matches:
        # Filter chỉ lấy links từ tiki.vn và có chứa product
        if 'tiki.vn' in url or url.startswith('/'):
            # Normalize URL
            if url.startswith('/'):
                full_url = urljoin(TIKI_BASE_URL, url)
            else:
                full_url = url
            
            # Check nếu là product link - pattern /pXXXXX hoặc ?id=XXXXX
            product_id = extract_product_id(url)
            is_product = product_id is not None
            
            # Exclude non-product links
            exclude_keywords = [
                'search?', 'checkout', 'cart', 'hotro', 'mailto:', 
                'javascript:', 'data:', 'account', 'login', 'register',
                'help', 'about', 'contact', 'policy', 'terms',
                '/c',  # Category links
                'category', 'danh-muc'
            ]
            is_excluded = any(kw in url.lower() for kw in exclude_keywords)
            
            # Include nếu là product
            if is_product and not is_excluded:
                products.append({
                    'name': name.strip(),
                    'url': full_url,
                    'product_id': product_id,
                    'category_id': category_id,
                    'category_name': category_name,
                    'slug': url.split('/')[-1].split('?')[0] if '/' in url else None
                })
    
    return products


def extract_products_from_html(html_text: str, category_id: str = None, category_name: str = None) -> List[Dict[str, Any]]:
    """
    Extract product links từ HTML
    
    Args:
        html_text: HTML content từ Firecrawl
        category_id: ID của category (optional)
        category_name: Tên category (optional)
    
    Returns:
        List of products với 'name', 'url', 'product_id'
    """
    products = []
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # Tìm tất cả links
    links = soup.find_all('a', href=True)
    
    for link in links:
        href = link.get('href', '')
        text = link.get_text(strip=True)
        
        # Filter product links
        if href and ('tiki.vn' in href or href.startswith('/')):
            # Check product pattern
            product_id = extract_product_id(href)
            is_product = product_id is not None
            
            # Exclude non-product links
            exclude_keywords = [
                'search?', 'checkout', 'cart', 'hotro', 'mailto:', 
                'javascript:', 'data:', 'account', 'login', 'register',
                '/c',  # Category links
                'category', 'danh-muc'
            ]
            is_excluded = any(kw in href.lower() for kw in exclude_keywords)
            
            if is_product and not is_excluded:
                if href.startswith('/'):
                    full_url = urljoin(TIKI_BASE_URL, href)
                else:
                    full_url = href
                
                products.append({
                    'name': text or full_url,
                    'url': full_url,
                    'product_id': product_id,
                    'category_id': category_id,
                    'category_name': category_name,
                    'slug': href.split('/')[-1].split('?')[0] if '/' in href else None
                })
    
    return products


def parse_firecrawl_products(response_data: Dict[str, Any], category_id: str = None, category_name: str = None) -> List[Dict[str, Any]]:
    """
    Parse response từ Firecrawl và extract products
    
    Args:
        response_data: Response từ Firecrawl API
        category_id: ID của category (optional)
        category_name: Tên category (optional)
    
    Returns:
        List of unique products
    """
    products = []
    
    # Check nếu có markdown
    if 'data' in response_data and 'markdown' in response_data['data']:
        markdown = response_data['data']['markdown']
        products.extend(extract_products_from_markdown(markdown, category_id, category_name))
    
    # Check nếu có HTML
    if 'data' in response_data and 'html' in response_data['data']:
        html = response_data['data']['html']
        products.extend(extract_products_from_html(html, category_id, category_name))
    
    # Remove duplicates - ưu tiên theo product_id và URL
    seen_urls = set()
    seen_ids = set()
    unique_products = []
    
    for product in products:
        url = product['url']
        product_id = product.get('product_id')
        
        # Remove query params để so sánh
        clean_url = url.split('?')[0]
        
        # Check duplicate by URL hoặc product_id
        is_duplicate = (
            clean_url in seen_urls or 
            (product_id and product_id in seen_ids)
        )
        
        if not is_duplicate:
            seen_urls.add(clean_url)
            if product_id:
                seen_ids.add(product_id)
            unique_products.append(product)
    
    # Sort by product_id để dễ đọc
    unique_products.sort(key=lambda x: int(x.get('product_id', 0)) if x.get('product_id') else 999999)
    
    return unique_products


def crawl_products_from_category(
    category_url: str,
    category_id: str = None,
    category_name: str = None,
    max_products: int = None,
    timeout: int = 60
) -> List[Dict[str, Any]]:
    """
    Crawl products từ một category URL
    
    Args:
        category_url: URL của category cần crawl products
        category_id: ID của category (optional)
        category_name: Tên của category (optional)
        max_products: Giới hạn số lượng products (None = tất cả)
        timeout: Timeout cho request (seconds)
    
    Returns:
        List of products với thông tin đầy đủ
    """
    payload = {
        "url": category_url,
        "onlyMainContent": True,
        "maxAge": 172800000,  # 2 days
        "parsers": [],
        "formats": ["html", "markdown"]
    }
    
    try:
        response = requests.post(
            f"{FIRECRAWL_API_URL}/v0/scrape",
            json=payload,
            timeout=timeout
        )
        response.raise_for_status()
        response_data = response.json()
        
        # Parse products từ response
        products = parse_firecrawl_products(response_data, category_id, category_name)
        
        # Giới hạn số lượng nếu cần
        if max_products and len(products) > max_products:
            products = products[:max_products]
        
        return products
        
    except requests.exceptions.RequestException as e:
        try:
            print(f"⚠️  Lỗi khi crawl products từ {category_url}: {e}")
        except (ValueError, OSError):
            try:
                print(f"Error crawling products from {category_url}: {e}", file=sys.stderr)
            except:
                pass
        return []
    except Exception as e:
        try:
            print(f"⚠️  Lỗi không mong đợi: {e}")
        except (ValueError, OSError):
            try:
                print(f"Unexpected error: {e}", file=sys.stderr)
            except:
                pass
        return []


def crawl_products_from_categories(
    categories: List[Dict[str, Any]],
    max_products_per_category: int = None,
    max_categories: int = None,
    timeout: int = 60
) -> List[Dict[str, Any]]:
    """
    Crawl products từ nhiều categories
    
    Args:
        categories: List các categories (dict với 'url', 'category_id', 'name')
        max_products_per_category: Giới hạn products mỗi category
        max_categories: Giới hạn số categories để crawl
        timeout: Timeout cho mỗi request
    
    Returns:
        List tất cả products từ các categories
    """
    all_products = []
    
    # Giới hạn số categories nếu cần
    categories_to_crawl = categories
    if max_categories:
        categories_to_crawl = categories[:max_categories]
    
    total = len(categories_to_crawl)
    
    for i, category in enumerate(categories_to_crawl, 1):
        category_url = category.get('url', '')
        category_id = category.get('category_id', '')
        category_name = category.get('name', 'N/A')
        
        if not category_url:
            continue
        
        try:
            print(f"[{i}/{total}] 📦 Crawling products từ: {category_name} (ID: {category_id})")
            print(f"   URL: {category_url}")
        except (ValueError, OSError):
            try:
                print(f"[{i}/{total}] Crawling products from: {category_name} (ID: {category_id})", file=sys.stderr)
            except:
                pass
        
        products = crawl_products_from_category(
            category_url=category_url,
            category_id=category_id,
            category_name=category_name,
            max_products=max_products_per_category,
            timeout=timeout
        )
        
        if products:
            try:
                print(f"   ✓ Tìm thấy {len(products)} products")
            except (ValueError, OSError):
                try:
                    print(f"   Found {len(products)} products", file=sys.stderr)
                except:
                    pass
            all_products.extend(products)
        else:
            try:
                print(f"   - Không tìm thấy products")
            except (ValueError, OSError):
                try:
                    print(f"   - No products found", file=sys.stderr)
                except:
                    pass
    
    return all_products


def save_products_to_json(products: List[Dict[str, Any]], output_file: str):
    """
    Lưu products vào file JSON
    
    Args:
        products: List các products
        output_file: Đường dẫn file output
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(products, f, indent=2, ensure_ascii=False)
    
    try:
        print(f"💾 Đã lưu {len(products)} products vào: {output_file}")
    except (ValueError, OSError):
        try:
            print(f"Saved {len(products)} products to: {output_file}", file=sys.stderr)
        except:
            pass


def load_products_from_json(json_file: str) -> List[Dict[str, Any]]:
    """
    Load products từ file JSON
    
    Args:
        json_file: Đường dẫn file JSON
    
    Returns:
        List các products
    """
    if not os.path.exists(json_file):
        return []
    
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        try:
            print(f"⚠️  Lỗi khi parse JSON: {e}")
        except (ValueError, OSError):
            try:
                print(f"Error parsing JSON: {e}", file=sys.stderr)
            except:
                pass
        return []

