
import os
import sys
import re
import requests
import json
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Fix encoding on Windows
if sys.platform == "win32":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')


# Cấu hình
FIRECRAWL_API_URL = os.getenv("FIRECRAWL_API_URL", "http://localhost:3002")
TIKI_BASE_URL = "https://tiki.vn"


def extract_category_id(url):
    """Extract category ID từ URL (pattern: /c1234)"""
    match = re.search(r'/c(\d+)', url)
    return match.group(1) if match else None


def extract_categories_from_markdown(markdown_text):
    """
    Extract category links từ markdown text
    Returns: List of dicts với 'name', 'url', 'category_id'
    """
    categories = []
    
    # Pattern để tìm markdown links: [text](url)
    link_pattern = r'\[([^\]]+)\]\(([^)]+)\)'
    matches = re.findall(link_pattern, markdown_text)
    
    for name, url in matches:
        # Filter chỉ lấy links từ tiki.vn và có chứa "danh-muc" hoặc category
        if 'tiki.vn' in url or url.startswith('/'):
            # Normalize URL
            if url.startswith('/'):
                full_url = urljoin(TIKI_BASE_URL, url)
            else:
                full_url = url
            
            # Check nếu là category link - chỉ lấy links có pattern /cXXXX
            # Pattern: /c1234 (category ID)
            category_id = extract_category_id(url)
            is_category = category_id is not None
            
            # Exclude non-category links
            exclude_keywords = [
                'search?', 'checkout', 'cart', 'hotro', 'mailto:', 
                'javascript:', 'data:', 'account', 'login', 'register',
                'help', 'about', 'contact', 'policy', 'terms'
            ]
            is_excluded = any(kw in url.lower() for kw in exclude_keywords)
            
            # Exclude nếu không có category ID
            if is_category and not is_excluded:
                categories.append({
                    'name': name.strip(),
                    'url': full_url,
                    'category_id': category_id,
                    'slug': url.split('/')[-1].split('?')[0]  # Extract slug
                })
    
    return categories


def extract_categories_from_html(html_text):
    """
    Extract category links từ HTML (nếu có trong response)
    """
    categories = []
    soup = BeautifulSoup(html_text, 'html.parser')
    
    # Tìm tất cả links
    links = soup.find_all('a', href=True)
    
    for link in links:
        href = link.get('href', '')
        text = link.get_text(strip=True)
        
        # Filter category links - chỉ lấy links có pattern /cXXXX
        if href and ('tiki.vn' in href or href.startswith('/')):
            # Check category pattern - chỉ lấy /cXXXX
            category_id = extract_category_id(href)
            is_category = category_id is not None
            
            # Exclude non-category links
            exclude_keywords = [
                'search?', 'checkout', 'cart', 'hotro', 'mailto:', 
                'javascript:', 'data:', 'account', 'login', 'register'
            ]
            is_excluded = any(kw in href.lower() for kw in exclude_keywords)
            
            if is_category and not is_excluded:
                if href.startswith('/'):
                    full_url = urljoin(TIKI_BASE_URL, href)
                else:
                    full_url = href
                
                categories.append({
                    'name': text or href,
                    'url': full_url,
                    'category_id': category_id,
                    'slug': href.split('/')[-1].split('?')[0]
                })
    
    return categories


def parse_firecrawl_response(response_data):
    """
    Parse response từ Firecrawl và extract categories
    """
    categories = []
    
    # Check nếu có markdown
    if 'data' in response_data and 'markdown' in response_data['data']:
        markdown = response_data['data']['markdown']
        categories.extend(extract_categories_from_markdown(markdown))
    
    # Check nếu có HTML
    if 'data' in response_data and 'html' in response_data['data']:
        html = response_data['data']['html']
        categories.extend(extract_categories_from_html(html))
    
    # Remove duplicates - ưu tiên theo category_id và URL
    seen_urls = set()
    seen_ids = set()
    unique_categories = []
    
    for cat in categories:
        url = cat['url']
        cat_id = cat.get('category_id')
        
        # Remove query params để so sánh
        clean_url = url.split('?')[0]
        
        # Check duplicate by URL hoặc category_id
        is_duplicate = (
            clean_url in seen_urls or 
            (cat_id and cat_id in seen_ids)
        )
        
        if not is_duplicate:
            seen_urls.add(clean_url)
            if cat_id:
                seen_ids.add(cat_id)
            unique_categories.append(cat)
    
    # Sort by category_id để dễ đọc
    unique_categories.sort(key=lambda x: int(x.get('category_id', 0)) if x.get('category_id') else 999999)
    
    return unique_categories


def crawl_sub_categories(category_url, parent_category_id=None, parent_name=None):
    """
    Crawl sub-categories từ một category URL
    
    Args:
        category_url: URL của category cần crawl sub-categories
        parent_category_id: ID của category cha (nếu có)
        parent_name: Tên của category cha (nếu có)
    
    Returns:
        List of sub-categories với parent_id
    """
    payload = {
        "url": category_url,
        "onlyMainContent": True,
        "maxAge": 172800000,
        "parsers": [],
        "formats": ["html"]
    }
    
    url = f"{FIRECRAWL_API_URL}/v2/scrape"
    
    try:
        response = requests.post(url, json=payload, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        sub_categories = parse_firecrawl_response(data)
        
        # Thêm parent_id và parent_name vào mỗi sub-category
        for sub_cat in sub_categories:
            sub_cat['parent_id'] = parent_category_id
            sub_cat['parent_name'] = parent_name
            sub_cat['parent_url'] = category_url
        
        return sub_categories
        
    except requests.exceptions.RequestException as e:
        print(f"  ⚠️  Lỗi khi crawl {category_url}: {e}")
        return []
    except Exception as e:
        print(f"  ⚠️  Lỗi: {e}")
        return []


def load_categories_from_json(json_file):
    """
    Load categories từ file JSON
    """
    try:
        with open(json_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"⚠️  File không tồn tại: {json_file}")
        return []
    except json.JSONDecodeError as e:
        print(f"⚠️  Lỗi khi parse JSON: {e}")
        return []


def build_hierarchical_structure(categories):
    """
    Chuyển đổi danh sách phẳng categories sang cấu trúc phân cấp (hierarchical)
    
    Args:
        categories: List các categories với parent_id
    
    Returns:
        List các categories ở level 1 với sub_categories được lồng bên trong
    """
    # Tạo dictionary để tra cứu nhanh theo category_id
    categories_dict = {}
    root_categories = []
    
    # Bước 1: Tạo dictionary và thêm field sub_categories cho mỗi category
    for cat in categories:
        cat_id = cat.get('category_id')
        if cat_id:
            cat_copy = cat.copy()
            cat_copy['sub_categories'] = []
            categories_dict[cat_id] = cat_copy
    
    # Bước 2: Phân loại categories theo level và xây dựng cấu trúc phân cấp
    for cat in categories:
        cat_id = cat.get('category_id')
        parent_id = cat.get('parent_id')
        
        if not cat_id:
            continue
        
        # Nếu không có parent_id hoặc parent_id là None -> đây là root category
        if not parent_id:
            if cat_id in categories_dict:
                root_categories.append(categories_dict[cat_id])
        else:
            # Nếu có parent_id, thêm vào sub_categories của parent
            if parent_id in categories_dict:
                if cat_id in categories_dict:
                    # Kiểm tra xem đã có trong sub_categories chưa để tránh duplicate
                    existing_ids = [sc.get('category_id') for sc in categories_dict[parent_id]['sub_categories']]
                    if cat_id not in existing_ids:
                        categories_dict[parent_id]['sub_categories'].append(categories_dict[cat_id])
    
    # Sắp xếp root categories theo category_id
    root_categories.sort(key=lambda x: int(x.get('category_id', 0)) if x.get('category_id') else 999999)
    
    # Sắp xếp sub_categories trong mỗi category
    def sort_subcategories(cat):
        if 'sub_categories' in cat and cat['sub_categories']:
            cat['sub_categories'].sort(key=lambda x: int(x.get('category_id', 0)) if x.get('category_id') else 999999)
            # Đệ quy sắp xếp sub_categories của sub_categories
            for sub_cat in cat['sub_categories']:
                sort_subcategories(sub_cat)
    
    for root_cat in root_categories:
        sort_subcategories(root_cat)
    
    return root_categories


def create_merged_categories_file():
    """
    Tạo file JSON hợp nhất với cấu trúc phân cấp từ tất cả các file categories
    """
    print("=" * 60)
    print("Tạo file JSON hợp nhất với cấu trúc phân cấp...")
    print("=" * 60)
    
    # Load tất cả categories từ file all_categories
    all_categories_file = "data/raw/tiki_all_categories.json"
    
    if not os.path.exists(all_categories_file):
        print(f"⚠️  File không tồn tại: {all_categories_file}")
        print("   Đang thử hợp nhất từ các file riêng lẻ...")
        
        # Thử hợp nhất từ các file riêng lẻ
        all_categories = []
        
        # Load level 1
        categories_file = "data/raw/tiki_categories.json"
        if os.path.exists(categories_file):
            parent_cats = load_categories_from_json(categories_file)
            for cat in parent_cats:
                cat_copy = cat.copy()
                cat_copy['parent_id'] = None
                cat_copy['parent_name'] = None
                cat_copy['parent_url'] = None
                all_categories.append(cat_copy)
        
        # Load level 2
        sub_categories_file = "data/raw/tiki_sub_categories.json"
        if os.path.exists(sub_categories_file):
            level2_cats = load_categories_from_json(sub_categories_file)
            all_categories.extend(level2_cats)
        
        # Load level 3
        level3_file = "data/raw/tiki_sub_categories_level3.json"
        if os.path.exists(level3_file):
            level3_cats = load_categories_from_json(level3_file)
            all_categories.extend(level3_cats)
        
        if not all_categories:
            print("⚠️  Không tìm thấy file categories nào để hợp nhất")
            return None
    else:
        all_categories = load_categories_from_json(all_categories_file)
    
    print(f"\n[1] Đã load {len(all_categories)} categories")
    
    # Xây dựng cấu trúc phân cấp
    print("[2] Đang xây dựng cấu trúc phân cấp...")
    hierarchical_categories = build_hierarchical_structure(all_categories)
    
    print(f"[3] Đã tạo cấu trúc phân cấp với {len(hierarchical_categories)} root categories")
    
    # Lưu file hợp nhất
    merged_file = "data/raw/tiki_categories_merged.json"
    os.makedirs(os.path.dirname(merged_file), exist_ok=True)
    
    with open(merged_file, 'w', encoding='utf-8') as f:
        json.dump(hierarchical_categories, f, indent=2, ensure_ascii=False)
    
    print(f"[4] Đã lưu file hợp nhất vào: {merged_file}")
    
    # Thống kê
    def count_categories(cats, level=1):
        total = len(cats)
        for cat in cats:
            if 'sub_categories' in cat and cat['sub_categories']:
                total += count_categories(cat['sub_categories'], level + 1)
        return total
    
    total_count = count_categories(hierarchical_categories)
    print(f"\n[5] Thống kê:")
    print(f"    - Root categories: {len(hierarchical_categories)}")
    print(f"    - Tổng số categories (bao gồm sub): {total_count}")
    
    return merged_file


def crawl_all_sub_categories(categories, max_categories=None):
    """
    Crawl sub-categories từ tất cả các categories
    
    Args:
        categories: List các categories cần crawl sub-categories
        max_categories: Giới hạn số lượng categories để crawl (None = tất cả)
    
    Returns:
        List tất cả sub-categories với parent_id
    """
    all_sub_categories = []
    total = len(categories) if max_categories is None else min(max_categories, len(categories))
    
    print(f"\n[5] Đang crawl sub-categories từ {total} categories...")
    print("-" * 60)
    
    for i, cat in enumerate(categories[:total], 1):
        cat_name = cat.get('name', 'N/A')
        cat_url = cat.get('url', '')
        cat_id = cat.get('category_id', '')
        
        print(f"\n[{i}/{total}] Đang crawl sub-categories của: {cat_name}")
        print(f"   URL: {cat_url}")
        
        sub_cats = crawl_sub_categories(
            category_url=cat_url,
            parent_category_id=cat_id,
            parent_name=cat_name
        )
        
        if sub_cats:
            print(f"   ✓ Tìm thấy {len(sub_cats)} sub-categories")
            all_sub_categories.extend(sub_cats)
        else:
            print(f"   - Không tìm thấy sub-categories")
    
    # Remove duplicates dựa trên category_id
    seen_ids = set()
    unique_sub_categories = []
    
    for sub_cat in all_sub_categories:
        cat_id = sub_cat.get('category_id')
        if cat_id and cat_id not in seen_ids:
            seen_ids.add(cat_id)
            unique_sub_categories.append(sub_cat)
    
    print(f"\n[6] Tổng cộng tìm thấy {len(unique_sub_categories)} unique sub-categories")
    
    return unique_sub_categories


def crawl_deep_sub_categories_from_file(sub_categories_file, max_categories=None, exclude_self_ref=True):
    """
    Crawl tiếp các sub-categories từ file sub_categories đã có
    
    Args:
        sub_categories_file: Đường dẫn đến file sub_categories.json
        max_categories: Giới hạn số lượng categories để crawl (None = tất cả)
        exclude_self_ref: Loại bỏ các category có parent_id trùng với category_id (self-reference)
    
    Returns:
        List các sub-categories level tiếp theo
    """
    print("=" * 60)
    print("Crawling deep sub-categories từ file...")
    print("=" * 60)
    
    # Load sub-categories từ file
    sub_categories = load_categories_from_json(sub_categories_file)
    
    if not sub_categories:
        print("⚠️  Không tìm thấy sub-categories trong file")
        return []
    
    print(f"\n[0] Đã load {len(sub_categories)} sub-categories từ file")
    
    # Lọc bỏ các self-reference (category có parent_id == category_id)
    if exclude_self_ref:
        filtered_categories = []
        for cat in sub_categories:
            cat_id = cat.get('category_id')
            parent_id = cat.get('parent_id')
            # Loại bỏ nếu parent_id == category_id (self-reference)
            if cat_id and parent_id and cat_id != parent_id:
                filtered_categories.append(cat)
            elif not parent_id or not cat_id:
                filtered_categories.append(cat)
        
        print(f"[1] Sau khi lọc self-reference: {len(filtered_categories)} categories")
        sub_categories = filtered_categories
    
    # Crawl tiếp từ các sub-categories
    deep_sub_categories = crawl_all_sub_categories(sub_categories, max_categories)
    
    return deep_sub_categories


def main():
    """Main function để crawl và extract categories"""
    
    print("=" * 60)
    print("Crawling Tiki.vn và extract category links...")
    print("=" * 60)
    
    # Kiểm tra xem có file sub_categories chưa để crawl tiếp
    sub_categories_file = "data/raw/tiki_sub_categories.json"
    
    if os.path.exists(sub_categories_file):
        print("\n[INFO] Tìm thấy file sub_categories, đang crawl tiếp level 3...")
        
        # Crawl tiếp từ sub_categories
        deep_sub_categories = crawl_deep_sub_categories_from_file(
            sub_categories_file,
            max_categories=None,  # Crawl tất cả
            exclude_self_ref=True
        )
        
        if deep_sub_categories:
            # Lưu deep sub-categories
            deep_sub_categories_file = "data/raw/tiki_sub_categories_level3.json"
            os.makedirs(os.path.dirname(deep_sub_categories_file), exist_ok=True)
            
            with open(deep_sub_categories_file, 'w', encoding='utf-8') as f:
                json.dump(deep_sub_categories, f, indent=2, ensure_ascii=False)
            
            print(f"\n[7] Đã lưu deep sub-categories vào: {deep_sub_categories_file}")
            
            # Load tất cả categories đã có
            all_existing = []
            
            # Load parent categories
            categories_file = "data/raw/tiki_categories.json"
            if os.path.exists(categories_file):
                parent_cats = load_categories_from_json(categories_file)
                for cat in parent_cats:
                    cat_copy = cat.copy()
                    cat_copy['parent_id'] = None
                    cat_copy['parent_name'] = None
                    cat_copy['parent_url'] = None
                    all_existing.append(cat_copy)
            
            # Load level 2 sub-categories
            level2_cats = load_categories_from_json(sub_categories_file)
            all_existing.extend(level2_cats)
            
            # Thêm level 3 sub-categories
            all_existing.extend(deep_sub_categories)
            
            # Lưu file tổng hợp
            all_categories_file = "data/raw/tiki_all_categories.json"
            with open(all_categories_file, 'w', encoding='utf-8') as f:
                json.dump(all_existing, f, indent=2, ensure_ascii=False)
            
            # Đếm số lượng theo level
            level1_count = len([c for c in all_existing if c.get('parent_id') is None])
            level3_ids = set(x.get('category_id') for x in deep_sub_categories if x.get('category_id'))
            # Level 2: có parent_id nhưng không có trong level3_ids
            level2_count = len([c for c in level2_cats if c.get('category_id') not in level3_ids])
            
            print(f"[8] Đã lưu tất cả categories (level 1 + 2 + 3) vào: {all_categories_file}")
            print(f"    Tổng cộng: {len(all_existing)} categories")
            print(f"    - Level 1 (parent): {level1_count}")
            print(f"    - Level 2: {level2_count}")
            print(f"    - Level 3: {len(deep_sub_categories)}")
            
            # Tạo file hợp nhất với cấu trúc phân cấp
            print("\n" + "=" * 60)
            create_merged_categories_file()
        
        return deep_sub_categories if deep_sub_categories else []
    
    # Nếu chưa có file sub_categories, crawl từ đầu
    categories_file = "data/raw/tiki_categories.json"
    categories = []
    
    if os.path.exists(categories_file):
        print("\n[0] Đã tìm thấy file categories, đang load...")
        categories = load_categories_from_json(categories_file)
        print(f"   Đã load {len(categories)} categories từ file")
    else:
        # Crawl với Firecrawl nếu chưa có file
        payload = {
            "url": "https://tiki.vn/",
            "onlyMainContent": True,
            "maxAge": 172800000,
            "parsers": [],
            "formats": ["html"]
        }
        
        url = f"{FIRECRAWL_API_URL}/v2/scrape"
        
        try:
            print("\n[1] Đang crawl từ Firecrawl...")
            response = requests.post(url, json=payload, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            print("[2] Đang parse và extract categories...")
            categories = parse_firecrawl_response(data)
            
            print(f"\n[3] Tìm thấy {len(categories)} categories:")
            print("-" * 60)
            
            for i, cat in enumerate(categories, 1):
                print(f"{i}. {cat['name']}")
                print(f"   ID: {cat.get('category_id', 'N/A')}")
                print(f"   URL: {cat['url']}")
                print()
            
            # Save to JSON
            os.makedirs(os.path.dirname(categories_file), exist_ok=True)
            
            with open(categories_file, 'w', encoding='utf-8') as f:
                json.dump(categories, f, indent=2, ensure_ascii=False)
            
            print(f"[4] Đã lưu vào: {categories_file}")
            
        except requests.exceptions.RequestException as e:
            print(f"Error khi crawl: {e}")
            return []
        except Exception as e:
            print(f"Error: {e}")
            return []
    
    # Crawl sub-categories từ các categories đã có
    if categories:
        sub_categories = crawl_all_sub_categories(categories)
        
        # Lưu sub-categories
        if sub_categories:
            os.makedirs(os.path.dirname(sub_categories_file), exist_ok=True)
            
            with open(sub_categories_file, 'w', encoding='utf-8') as f:
                json.dump(sub_categories, f, indent=2, ensure_ascii=False)
            
            print(f"\n[7] Đã lưu sub-categories vào: {sub_categories_file}")
            
            # Tạo file tổng hợp tất cả categories (parent + sub)
            all_categories = []
            
            # Thêm parent categories (không có parent_id)
            for cat in categories:
                cat_copy = cat.copy()
                cat_copy['parent_id'] = None
                cat_copy['parent_name'] = None
                cat_copy['parent_url'] = None
                all_categories.append(cat_copy)
            
            # Thêm sub-categories
            all_categories.extend(sub_categories)
            
            all_categories_file = "data/raw/tiki_all_categories.json"
            with open(all_categories_file, 'w', encoding='utf-8') as f:
                json.dump(all_categories, f, indent=2, ensure_ascii=False)
            
            print(f"[8] Đã lưu tất cả categories (parent + sub) vào: {all_categories_file}")
            print(f"    Tổng cộng: {len(all_categories)} categories")
            
            # Tạo file hợp nhất với cấu trúc phân cấp
            print("\n" + "=" * 60)
            create_merged_categories_file()
        
        return all_categories if sub_categories else categories
    
    return categories


if __name__ == "__main__":
    main()