#!/usr/bin/env python
import requests
from bs4 import BeautifulSoup
import json

# Test lấy một product page để xem breadcrumb
url = 'https://tiki.vn/p/68065469'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}

try:
    resp = requests.get(url, headers=headers, timeout=10)
    soup = BeautifulSoup(resp.content, 'html.parser')
    
    # Tìm breadcrumb
    breadcrumbs = soup.select('[data-view-id="pdp_breadcrumb"] a')
    if not breadcrumbs:
        breadcrumbs = soup.select('.breadcrumb a')
    if not breadcrumbs:
        breadcrumbs = soup.select('[class*="breadcrumb"] a')
    
    print('=== Breadcrumb Links ===')
    paths = []
    for i, b in enumerate(breadcrumbs):
        text = b.get_text(strip=True)
        href = b.get('href', '')
        print(f'{i}. {text}')
        print(f'   href: {href[:60] if href else "N/A"}')
        paths.append(text)
    
    print(f'\nTotal: {len(breadcrumbs)} links')
    print(f'Path: {json.dumps(paths)}')
    
except Exception as e:
    print(f'Error: {e}')
    import traceback
    traceback.print_exc()
