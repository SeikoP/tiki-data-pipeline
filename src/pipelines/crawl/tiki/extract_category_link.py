
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
    
    Thuật toán:
    1. Tạo dict để tra cứu nhanh các categories
    2. Đánh dấu root categories (parent_id = None)
    3. Xây dựng quan hệ parent-child
    4. Kiểm tra và loại bỏ circular references
    5. Sắp xếp theo category_id
    
    Args:
        categories: List các categories với parent_id
    
    Returns:
        List các categories ở level 1 với sub_categories được lồng bên trong
    """
    if not categories:
        return []
    
    # ===== BƯỚC 0: REMOVE DUPLICATES =====
    # Giữ lại TOÀN BỘ dữ liệu, chỉ remove duplicates
    seen_ids = set()
    unique_categories = []
    duplicates_removed = 0
    skipped_no_id = 0
    
    for cat in categories:
        cat_id = cat.get('category_id')
        
        # Categories không có category_id - vẫn giữ lại nhưng track
        if not cat_id:
            skipped_no_id += 1
            unique_categories.append(cat)  # Keep it for manual review
            continue
        
        # Duplicate - skip
        if cat_id in seen_ids:
            duplicates_removed += 1
            continue
        
        seen_ids.add(cat_id)
        unique_categories.append(cat)
    
    if duplicates_removed > 0:
        print(f"  ⚠️  Removed {duplicates_removed} duplicate categories (kept latest)")
    if skipped_no_id > 0:
        print(f"  ⚠️  Found {skipped_no_id} categories without category_id (kept for review)")
    
    categories = unique_categories
    
    # ===== BƯỚC 1: CHUẨN BỊ DỮ LIỆU =====
    categories_dict = {}
    root_category_ids = set()
    parent_child_map = {}  # {parent_id: [child_ids]}
    issues = {'circular_refs': [], 'orphaned': [], 'missing_parents': []}
    
    # Tạo dictionary lookup và initialize
    for cat in categories:
        cat_id = cat.get('category_id')
        if not cat_id:
            continue
        
        cat_copy = cat.copy()
        cat_copy['sub_categories'] = []
        categories_dict[cat_id] = cat_copy
        
        parent_id = cat.get('parent_id')
        
        # Xác định root categories (không có parent)
        if not parent_id:
            root_category_ids.add(cat_id)
        else:
            # Track parent-child relationship
            if parent_id not in parent_child_map:
                parent_child_map[parent_id] = []
            parent_child_map[parent_id].append(cat_id)
    
    # ===== BƯỚC 2: XỬ LÝ CIRCULAR REFERENCES =====
    def has_circular_reference(parent_id, child_id, visited=None, max_depth=50):
        """Kiểm tra nếu thêm child vào parent sẽ tạo circular reference"""
        if visited is None:
            visited = set()
        
        if parent_id == child_id:
            return True
        
        if parent_id in visited or len(visited) >= max_depth:
            return False
        
        visited.add(parent_id)
        
        # Tìm parent của parent_id hiện tại
        if parent_id in categories_dict:
            parent_of_parent = categories_dict[parent_id].get('parent_id')
            if parent_of_parent and parent_of_parent == child_id:
                return True
            if parent_of_parent:
                return has_circular_reference(parent_of_parent, child_id, visited.copy(), max_depth)
        
        return False
    
    # Validate và GIỮ LẠI tất cả categories hợp lệ
    validated_categories = []
    rejected_categories = []
    
    for cat in categories:
        cat_id = cat.get('category_id')
        parent_id = cat.get('parent_id')
        
        # Categories không có category_id - giữ lại với warning
        if not cat_id:
            issues['orphaned'].append(f"No category_id: {cat}")
            validated_categories.append(cat)  # Keep it
            continue
        
        # Self-references - REJECT (không thể fix)
        if parent_id == cat_id:
            issues['circular_refs'].append(f"Self-reference: {cat_id} -> {cat_id}")
            rejected_categories.append((cat, f"Self-reference: {cat_id}"))
            continue
        
        # Circular references - REJECT (không thể fix)
        if parent_id and has_circular_reference(parent_id, cat_id):
            issues['circular_refs'].append(f"Circular: {parent_id} <-> {cat_id}")
            rejected_categories.append((cat, f"Circular ref with {parent_id}"))
            continue
        
        # Missing parent - KEEP NHƯ ROOT (có thể fix bằng cách treat as root)
        if parent_id and parent_id not in categories_dict:
            issues['missing_parents'].append(f"Missing parent {parent_id} for {cat_id}, treating as root")
            cat_copy = cat.copy()
            cat_copy['parent_id'] = None
            cat_copy['parent_name'] = None
            cat_copy['parent_url'] = None
            cat_copy['sub_categories'] = []
            validated_categories.append(cat_copy)
            root_category_ids.add(cat_id)
        else:
            # All other valid categories
            validated_categories.append(cat)
    
    # ===== BƯỚC 3: XÂY DỰNG QUAN HỆ PARENT-CHILD =====
    categories_dict = {}
    orphaned_categories = []  # Track categories without ID
    
    for cat in validated_categories:
        cat_id = cat.get('category_id')
        if cat_id:
            cat_copy = cat.copy()
            cat_copy['sub_categories'] = []
            categories_dict[cat_id] = cat_copy
        else:
            # Keep track of categories without ID (orphaned)
            orphaned_categories.append(cat)
    
    # Xây dựng parent-child relationships - GIỮ LẠI toàn bộ dữ liệu hợp lệ
    for cat in validated_categories:
        cat_id = cat.get('category_id')
        parent_id = cat.get('parent_id')
        
        # Skip chỉ nếu không có cat_id hoặc không có parent_id (là root)
        if not cat_id:
            continue  # Không thể xử lý, bỏ qua
        
        if not parent_id:
            continue  # Là root, đã xử lý ở bước 1
        
        # Thêm cat vào sub_categories của parent
        if cat_id in categories_dict and parent_id in categories_dict:
            # Check xem đã có chưa để tránh duplicate
            existing_ids = {sc.get('category_id') for sc in categories_dict[parent_id]['sub_categories']}
            if cat_id not in existing_ids:
                categories_dict[parent_id]['sub_categories'].append(categories_dict[cat_id])
        elif cat_id in categories_dict and parent_id not in categories_dict:
            # Parent không tồn tại nhưng category có ID - đã được treat as root ở bước trước
            pass  # OK, đã xử lý
    
    # ===== BƯỚC 4: EXTRACT ROOT CATEGORIES =====
    root_categories = []
    for cat_id in root_category_ids:
        if cat_id in categories_dict:
            root_categories.append(categories_dict[cat_id])
    
    # Thêm orphaned categories (không có category_id) vào root
    # để không mất dữ liệu
    for orphan in orphaned_categories:
        orphan_copy = orphan.copy()
        orphan_copy['sub_categories'] = []
        root_categories.append(orphan_copy)
    
    # ===== BƯỚC 5: SẮP XẾP =====
    def get_sort_key(cat):
        """Extract sort key từ category_id"""
        try:
            return int(cat.get('category_id', 999999))
        except (ValueError, TypeError):
            return 999999
    
    def sort_tree(cats, max_depth=100, current_depth=0):
        """Sắp xếp cây categories recursively"""
        if current_depth >= max_depth or not cats:
            return
        
        # Sắp xếp list hiện tại
        cats.sort(key=get_sort_key)
        
        # Đệ quy sắp xếp sub_categories
        for cat in cats:
            if 'sub_categories' in cat and cat['sub_categories']:
                sort_tree(cat['sub_categories'], max_depth, current_depth + 1)
    
    sort_tree(root_categories)
    
    # ===== BƯỚC 6: REPORT =====
    if issues['circular_refs'] or issues['missing_parents'] or issues['orphaned']:
        print("\n⚠️  Issues found during hierarchy building:")
        if issues['circular_refs']:
            for issue in issues['circular_refs'][:5]:
                print(f"  - Circular ref: {issue}")
            if len(issues['circular_refs']) > 5:
                print(f"  ... and {len(issues['circular_refs']) - 5} more")
        
        if issues['missing_parents']:
            print(f"  - Missing parents: {len(issues['missing_parents'])} categories")
        
        if issues['orphaned']:
            print(f"  - Orphaned: {len(issues['orphaned'])} categories")
    
    return root_categories




def validate_hierarchical_structure(hierarchical_categories, all_categories):
    """
    Validate cấu trúc phân cấp - kiểm tra:
    1. Tất cả categories đều được bao gồm
    2. Không có duplicate (cùng category_id xuất hiện 2 lần)
    3. parent_id và category_id đúng match
    4. Không có circular references
    
    Args:
        hierarchical_categories: Cấu trúc phân cấp đã build
        all_categories: Danh sách tất cả categories ban đầu
    
    Returns:
        dict: {
            'is_valid': bool,
            'stats': {...},
            'errors': [...]
        }
    """
    errors = []
    collected_ids = set()
    collected_categories = []
    
    def collect_all(cats):
        """Collect all categories from hierarchical structure"""
        for cat in cats:
            cat_id = cat.get('category_id')
            if cat_id:
                if cat_id in collected_ids:
                    errors.append(f"Duplicate category_id found: {cat_id}")
                else:
                    collected_ids.add(cat_id)
                    collected_categories.append(cat)
            
            # Đệ quy collect sub_categories
            if cat.get('sub_categories'):
                collect_all(cat['sub_categories'])
    
    collect_all(hierarchical_categories)
    
    # Kiểm tra xem có categories bị mất không
    original_ids = {cat.get('category_id') for cat in all_categories if cat.get('category_id')}
    missing_ids = original_ids - collected_ids
    
    if missing_ids:
        errors.append(f"Missing {len(missing_ids)} categories in hierarchy")
        # Show first 5
        for cat_id in list(missing_ids)[:5]:
            errors.append(f"  - Missing: {cat_id}")
        if len(missing_ids) > 5:
            errors.append(f"  ... and {len(missing_ids) - 5} more")
    
    # Kiểm tra parent_id đúng match
    for cat in collected_categories:
        cat_id = cat.get('category_id')
        for sub_cat in cat.get('sub_categories', []):
            sub_id = sub_cat.get('category_id')
            sub_parent_id = sub_cat.get('parent_id')
            
            # Nếu parent_id trong sub_cat khác với parent (category hiện tại), warning
            if sub_parent_id and str(sub_parent_id) != str(cat_id):
                errors.append(f"Mismatch parent_id: {sub_id} has parent_id={sub_parent_id} but is under {cat_id}")
    
    # Kiểm tra circular references
    def check_circular(cat, path=None):
        """Check for circular references in tree"""
        if path is None:
            path = []
        
        cat_id = cat.get('category_id')
        if cat_id in path:
            errors.append(f"Circular reference detected: {' -> '.join(path)} -> {cat_id}")
            return
        
        new_path = path + [cat_id]
        for sub_cat in cat.get('sub_categories', []):
            check_circular(sub_cat, new_path)
    
    for root_cat in hierarchical_categories:
        check_circular(root_cat)
    
    # Stats
    stats = {
        'total_collected': len(collected_ids),
        'total_original': len(original_ids),
        'total_missing': len(missing_ids),
        'total_root': len(hierarchical_categories),
        'max_depth': _get_max_depth(hierarchical_categories),
        'total_errors': len(errors)
    }
    
    return {
        'is_valid': len(errors) == 0,
        'stats': stats,
        'errors': errors
    }


def _get_max_depth(cats, current_depth=1):
    """Get maximum depth of hierarchical structure"""
    if not cats:
        return current_depth - 1
    
    max_depth = current_depth
    for cat in cats:
        if cat.get('sub_categories'):
            depth = _get_max_depth(cat['sub_categories'], current_depth + 1)
            max_depth = max(max_depth, depth)
    
    return max_depth


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
    
    # Validate cấu trúc phân cấp
    print("[4] Đang validate cấu trúc phân cấp...")
    validation_result = validate_hierarchical_structure(hierarchical_categories, all_categories)
    
    print(f"\n📊 Validation Results:")
    print(f"    - Valid: {validation_result['is_valid']}")
    print(f"    - Total collected: {validation_result['stats']['total_collected']}")
    print(f"    - Total original: {validation_result['stats']['total_original']}")
    print(f"    - Missing: {validation_result['stats']['total_missing']}")
    print(f"    - Root categories: {validation_result['stats']['total_root']}")
    print(f"    - Max depth: {validation_result['stats']['max_depth']}")
    print(f"    - Errors: {validation_result['stats']['total_errors']}")
    
    if validation_result['errors']:
        print(f"\n⚠️  Issues found:")
        for error in validation_result['errors'][:10]:
            print(f"    - {error}")
        if len(validation_result['errors']) > 10:
            print(f"    ... and {len(validation_result['errors']) - 10} more")
    
    # Lưu file hợp nhất
    merged_file = "data/raw/tiki_categories_merged.json"
    os.makedirs(os.path.dirname(merged_file), exist_ok=True)
    
    with open(merged_file, 'w', encoding='utf-8') as f:
        json.dump(hierarchical_categories, f, indent=2, ensure_ascii=False)
    
    print(f"\n[5] Đã lưu file hợp nhất vào: {merged_file}")
    
    return merged_file


def crawl_categories_recursive(
    categories, 
    visited_ids=None, 
    max_depth=10, 
    current_depth=0,
    max_categories_per_level=None,
    stats=None
):
    """
    Crawl sub-categories đệ quy để crawl tất cả các level
    
    Args:
        categories: List các categories cần crawl sub-categories
        visited_ids: Set các category_id đã crawl (để tránh duplicate)
        max_depth: Độ sâu tối đa để crawl (None = không giới hạn)
        current_depth: Độ sâu hiện tại
        max_categories_per_level: Giới hạn số categories mỗi level (None = tất cả)
        stats: Dict để track statistics
    
    Returns:
        List tất cả sub-categories với parent_id và level info
    """
    if visited_ids is None:
        visited_ids = set()
    
    if stats is None:
        stats = {
            'total_crawled': 0,
            'total_found': 0,
            'by_level': {},
            'errors': 0
        }
    
    all_sub_categories = []
    
    # Giới hạn số lượng categories nếu cần
    categories_to_crawl = categories
    if max_categories_per_level:
        categories_to_crawl = categories[:max_categories_per_level]
    
    total = len(categories_to_crawl)
    level_indicator = "  " * current_depth + "│" if current_depth > 0 else ""
    
    if current_depth == 0:
        print(f"\n[Level {current_depth}] Đang crawl {total} root categories...")
    else:
        print(f"\n{level_indicator}[Level {current_depth}] Đang crawl {total} categories...")
    
    for i, cat in enumerate(categories_to_crawl, 1):
        cat_id = cat.get('category_id')
        cat_name = cat.get('name', 'N/A')
        cat_url = cat.get('url', '')
        parent_id = cat.get('parent_id')
        parent_name = cat.get('parent_name', '')
        
        # Skip nếu đã crawl rồi
        if cat_id and cat_id in visited_ids:
            continue
        
        # Skip nếu có circular reference
        if parent_id and parent_id == cat_id:
            continue
        
        # Kiểm tra max depth
        if max_depth is not None and current_depth >= max_depth:
            if current_depth == max_depth:
                print(f"{level_indicator}⚠️  Đạt độ sâu tối đa {max_depth}, dừng crawl sâu hơn")
            continue
        
        stats['total_crawled'] += 1
        
        # Hiển thị progress
        indent = "  " * current_depth
        print(f"{indent}[{i}/{total}] 📂 {cat_name} (ID: {cat_id}, Level: {current_depth})")
        
        try:
            # Crawl sub-categories của category này
            sub_cats = crawl_sub_categories(
                category_url=cat_url,
                parent_category_id=cat_id,
                parent_name=cat_name
            )
            
            # Đánh dấu đã crawl
            if cat_id:
                visited_ids.add(cat_id)
            
            if sub_cats:
                # Filter bỏ circular references và duplicates
                valid_sub_cats = []
                for sub_cat in sub_cats:
                    sub_cat_id = sub_cat.get('category_id')
                    
                    # Skip nếu là self-reference
                    if sub_cat_id == cat_id:
                        continue
                    
                    # Skip nếu đã crawl rồi
                    if sub_cat_id and sub_cat_id in visited_ids:
                        continue
                    
                    # Thêm level info
                    sub_cat['level'] = current_depth + 1
                    valid_sub_cats.append(sub_cat)
                
                if valid_sub_cats:
                    print(f"{indent}   ✓ Tìm thấy {len(valid_sub_cats)} sub-categories")
                    all_sub_categories.extend(valid_sub_cats)
                    stats['total_found'] += len(valid_sub_cats)
                    
                    # Track by level
                    level_key = f"level_{current_depth + 1}"
                    if level_key not in stats['by_level']:
                        stats['by_level'][level_key] = 0
                    stats['by_level'][level_key] += len(valid_sub_cats)
                    
                    # Crawl đệ quy sub-categories
                    if current_depth + 1 < (max_depth if max_depth else 999):
                        deeper_cats = crawl_categories_recursive(
                            valid_sub_cats,
                            visited_ids=visited_ids,
                            max_depth=max_depth,
                            current_depth=current_depth + 1,
                            max_categories_per_level=max_categories_per_level,
                            stats=stats
                        )
                        all_sub_categories.extend(deeper_cats)
                else:
                    print(f"{indent}   - Không có sub-categories hợp lệ (có thể do duplicate/circular)")
            else:
                print(f"{indent}   - Không tìm thấy sub-categories")
                
        except Exception as e:
            stats['errors'] += 1
            print(f"{indent}   ✗ Lỗi: {e}")
            continue
    
    return all_sub_categories


def crawl_all_sub_categories(categories, max_categories=None, recursive=True, max_depth=10):
    """
    Crawl sub-categories từ tất cả các categories
    
    Args:
        categories: List các categories cần crawl sub-categories
        max_categories: Giới hạn số lượng categories để crawl (None = tất cả)
        recursive: Nếu True, crawl đệ quy tất cả các level. Nếu False, chỉ crawl 1 level
        max_depth: Độ sâu tối đa khi recursive=True
    
    Returns:
        List tất cả sub-categories với parent_id
    """
    if recursive:
        # Crawl đệ quy tất cả các level
        print(f"\n[5] Đang crawl sub-categories đệ quy từ {len(categories)} categories...")
        print(f"    Max depth: {max_depth if max_depth else 'unlimited'}")
        print("-" * 60)
        
        stats = {
            'total_crawled': 0,
            'total_found': 0,
            'by_level': {},
            'errors': 0
        }
        
        all_sub_categories = crawl_categories_recursive(
            categories,
            max_depth=max_depth,
            max_categories_per_level=max_categories,
            stats=stats
        )
        
        # Remove duplicates dựa trên category_id
        seen_ids = set()
        unique_sub_categories = []
        
        for sub_cat in all_sub_categories:
            cat_id = sub_cat.get('category_id')
            if cat_id and cat_id not in seen_ids:
                seen_ids.add(cat_id)
                unique_sub_categories.append(sub_cat)
        
        print(f"\n[6] Thống kê crawl:")
        print(f"    - Tổng categories đã crawl: {stats['total_crawled']}")
        print(f"    - Tổng sub-categories tìm thấy: {stats['total_found']}")
        print(f"    - Unique sub-categories: {len(unique_sub_categories)}")
        print(f"    - Lỗi: {stats['errors']}")
        if stats['by_level']:
            print(f"    - Phân bố theo level:")
            for level, count in sorted(stats['by_level'].items()):
                print(f"      {level}: {count} categories")
        
        return unique_sub_categories
    else:
        # Chỉ crawl 1 level (giữ nguyên logic cũ)
        all_sub_categories = []
        total = len(categories) if max_categories is None else min(max_categories, len(categories))
        
        print(f"\n[5] Đang crawl sub-categories từ {total} categories (1 level)...")
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
        # Sử dụng recursive=True để crawl tất cả các level
        # max_depth=None để không giới hạn độ sâu (hoặc set số cụ thể như 10)
        sub_categories = crawl_all_sub_categories(
            categories, 
            max_categories=None,
            recursive=True,  # Crawl đệ quy tất cả các level
            max_depth=None   # None = không giới hạn, hoặc set số như 10, 15
        )
        
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