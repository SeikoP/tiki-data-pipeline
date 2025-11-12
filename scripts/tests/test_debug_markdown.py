"""
Debug script để inspect Markdown từ Firecrawl
Xem xem markdown chứa gì và tại sao k extract được products
"""
import os
import sys
import json
import requests

# Thêm path để import modules
# Tính toán đường dẫn tuyệt đối đến src từ script hiện tại
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.abspath(os.path.join(_script_dir, '..', '..'))
_src_path = os.path.join(_project_root, 'src')
if _src_path not in sys.path:
    sys.path.insert(0, _src_path)

from pipelines.crawl.tiki.extract_products import extract_products_from_markdown

# Fix encoding on Windows
if sys.platform == "win32":
    try:
        if not hasattr(sys.stdout, 'buffer') or (hasattr(sys.stdout, 'encoding') and sys.stdout.encoding != 'utf-8'):
            import io
            if not isinstance(sys.stdout, io.TextIOWrapper):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    except (AttributeError, ValueError):
        pass


def safe_print(*args, **kwargs):
    """Safe print function"""
    try:
        print(*args, **kwargs)
    except (ValueError, OSError):
        try:
            print(*args, **kwargs, file=sys.stderr)
        except:
            pass


def crawl_and_inspect():
    """Crawl page từ Firecrawl và inspect markdown"""
    
    FIRECRAWL_API_URL = os.getenv("FIRECRAWL_API_URL", "http://localhost:3002")
    target_url = "https://tiki.vn/dien-thoai-smartphone/c1795"
    
    safe_print("=" * 70)
    safe_print("DEBUG: Inspect Markdown từ Firecrawl")
    safe_print("=" * 70)
    
    safe_print(f"\n📡 Crawling: {target_url}")
    
    payload = {
        "url": target_url,
        "onlyMainContent": True,
        "maxAge": 172800000,
        "formats": ["markdown"]
    }
    
    try:
        response = requests.post(
            f"{FIRECRAWL_API_URL}/v0/scrape",
            json=payload,
            timeout=60
        )
        response.raise_for_status()
        response_data = response.json()
        
        if 'data' not in response_data or 'markdown' not in response_data['data']:
            safe_print("❌ No markdown in response")
            return
        
        markdown = response_data['data']['markdown']
        
        safe_print(f"✓ Crawled successfully!")
        safe_print(f"✓ Markdown size: {len(markdown)} bytes")
        
        # ===== ANALYZE MARKDOWN =====
        safe_print("\n" + "-" * 70)
        safe_print("ANALYZE MARKDOWN CONTENT")
        safe_print("-" * 70)
        
        lines = markdown.split('\n')
        safe_print(f"Total lines: {len(lines)}")
        
        # Count links
        link_count = markdown.count('[')
        safe_print(f"Markdown links found: {link_count}")
        
        # Find product links
        import re
        product_links = re.findall(r'\[([^\]]+)\]\(([^)]*p\d+[^)]*)\)', markdown)
        category_links = re.findall(r'\[([^\]]+)\]\(([^)]*c\d+[^)]*)\)', markdown)
        
        safe_print(f"\n✓ Product links (pattern: /p + digits): {len(product_links)}")
        safe_print(f"✓ Category links (pattern: /c + digits): {len(category_links)}")
        
        # Show sample
        if product_links:
            safe_print("\nSample product links:")
            for i, (name, url) in enumerate(product_links[:5], 1):
                safe_print(f"  {i}. {name}")
                safe_print(f"     {url}")
        
        if category_links:
            safe_print("\nSample category links:")
            for i, (name, url) in enumerate(category_links[:3], 1):
                safe_print(f"  {i}. {name}")
                safe_print(f"     {url}")
        
        # ===== SAVE MARKDOWN FOR INSPECTION =====
        safe_print("\n" + "-" * 70)
        safe_print("SAVE MARKDOWN FOR INSPECTION")
        safe_print("-" * 70)
        
        output_dir = "data/raw/demo"
        os.makedirs(output_dir, exist_ok=True)
        
        markdown_file = os.path.join(output_dir, "crawled_page_markdown.md")
        with open(markdown_file, 'w', encoding='utf-8') as f:
            f.write(markdown)
        
        safe_print(f"✓ Saved markdown to: {markdown_file}")
        
        # ===== TRY EXTRACT WITH EXTRACTION FUNCTION =====
        safe_print("\n" + "-" * 70)
        safe_print("TRY EXTRACT PRODUCTS")
        safe_print("-" * 70)
        
        products = extract_products_from_markdown(
            markdown,
            category_id="1795",
            category_name="Điện Thoại Smartphone"
        )
        
        safe_print(f"\n✓ Extracted {len(products)} products")
        
        if products:
            safe_print("\nFirst 5 products:")
            for i, prod in enumerate(products[:5], 1):
                safe_print(f"  {i}. {prod['name']}")
                safe_print(f"     ID: {prod['product_id']}")
        else:
            safe_print("\n⚠️  No products extracted!")
            safe_print("\nAnalyzing why:")
            
            # Check for /p patterns in markdown
            all_plinks = re.findall(r'/p\d+', markdown)
            safe_print(f"  - Found {len(all_plinks)} /pXXXX patterns")
            
            # Check for product links in brackets
            all_bracket_links = re.findall(r'\[([^\]]+)\]\(([^)]+)\)', markdown)
            safe_print(f"  - Found {len(all_bracket_links)} markdown links total")
            
            # Sample of links found
            if all_bracket_links:
                safe_print("\n  Sample links in markdown:")
                for i, (name, url) in enumerate(all_bracket_links[:10], 1):
                    has_p = '/p' in url or '?id=' in url
                    status = "✓ PRODUCT" if has_p else "- OTHER"
                    safe_print(f"    {i}. [{status}] {name}")
                    safe_print(f"       {url}")
        
        # ===== SAVE FULL RESPONSE FOR DEBUG =====
        safe_print("\n" + "-" * 70)
        safe_print("SAVE FULL RESPONSE")
        safe_print("-" * 70)
        
        response_file = os.path.join(output_dir, "crawled_page_response.json")
        with open(response_file, 'w', encoding='utf-8') as f:
            json.dump(response_data, f, indent=2, ensure_ascii=False)
        
        safe_print(f"✓ Saved response to: {response_file}")
        
    except requests.exceptions.ConnectionError:
        safe_print("❌ Cannot connect to Firecrawl API")
    except Exception as e:
        safe_print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()


def main():
    safe_print("\n" + "=" * 70)
    safe_print("DEBUG SCRIPT - Inspect Firecrawl Response")
    safe_print("=" * 70)
    
    crawl_and_inspect()
    
    safe_print("\n" + "=" * 70)


if __name__ == "__main__":
    main()

