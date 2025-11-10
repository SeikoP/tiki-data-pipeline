"""
DAG để crawl products từ Tiki với parallel processing
Tối ưu cho crawl hàng triệu products
"""
from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import sys
import os
import json
import requests
from typing import List, Dict, Any

# Thêm path để import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from pipelines.crawl.tiki.config import get_config
from utils.rate_limiter import rate_limited, retry_with_backoff

config = get_config()

default_args = {
    'owner': 'tiki-data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=6),
}

with DAG(
    'tiki_crawl_products',
    default_args=default_args,
    description='Crawl products từ Tiki.vn với parallel processing',
    schedule=timedelta(days=1),  # Chạy hàng ngày
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tiki', 'crawl', 'products'],
    max_active_runs=1,
) as dag:

    @task
    def load_categories_for_crawl():
        """Load categories cần crawl products"""
        categories_file = config['data_paths']['all_categories']
        
        if not os.path.exists(categories_file):
            print(f"File {categories_file} không tồn tại")
            return []
        
        with open(categories_file, 'r', encoding='utf-8') as f:
            categories = json.load(f)
        
        # Chỉ lấy categories có URL hợp lệ (có thể filter theo level nếu cần)
        valid_categories = [
            cat for cat in categories
            if cat.get('url') and 'tiki.vn' in cat.get('url', '')
        ]
        
        print(f"Đã load {len(valid_categories)} categories để crawl products")
        return valid_categories

    @task
    def crawl_products_from_category(category: Dict[str, Any]):
        """
        Crawl products từ một category
        Sử dụng dynamic task mapping để chạy song song cho nhiều categories
        """
        category_id = category.get('category_id')
        category_url = category.get('url')
        category_name = category.get('name', 'N/A')
        
        print(f"Đang crawl products từ category: {category_name} (ID: {category_id})")
        
        @retry_with_backoff(
            max_retries=config['retry']['max_retries'],
            initial_delay=config['retry']['initial_delay'],
            backoff_factor=config['retry']['backoff_factor'],
            max_delay=config['retry']['max_delay'],
            exceptions=(requests.RequestException, Exception)
        )
        @rate_limited(
            max_per_minute=config['rate_limit']['max_requests_per_minute'],
            max_per_hour=config['rate_limit']['max_requests_per_hour']
        )
        def fetch_products():
            """Fetch products từ Tiki API hoặc scrape"""
            # TODO: Implement logic crawl products từ category
            # Có thể dùng Tiki API hoặc scrape HTML
            
            # Placeholder: Trả về empty list
            products = []
            
            # Ví dụ: Gọi Firecrawl API
            # payload = {
            #     "url": category_url,
            #     "onlyMainContent": True,
            #     "maxAge": config['crawl']['max_age'],
            #     "formats": ["html"]
            # }
            # 
            # response = requests.post(
            #     f"{config['FIRECRAWL_API_URL']}/v2/scrape",
            #     json=payload,
            #     timeout=config['crawl']['timeout']
            # )
            # response.raise_for_status()
            # data = response.json()
            # 
            # # Parse products từ HTML/markdown
            # products = parse_products_from_response(data)
            
            return products
        
        try:
            products = fetch_products()
            
            if products:
                print(f"  ✓ Tìm thấy {len(products)} products từ {category_name}")
                
                # Lưu vào file riêng cho category này
                output_dir = os.path.join(
                    config['data_paths']['raw'],
                    'tiki_products',
                    category_id or 'unknown'
                )
                os.makedirs(output_dir, exist_ok=True)
                
                output_file = os.path.join(output_dir, f"products_{category_id}.json")
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump({
                        'category_id': category_id,
                        'category_name': category_name,
                        'category_url': category_url,
                        'products': products,
                        'crawl_time': datetime.now().isoformat(),
                        'total_products': len(products)
                    }, f, indent=2, ensure_ascii=False)
                
                print(f"  Đã lưu vào: {output_file}")
            else:
                print(f"  - Không tìm thấy products từ {category_name}")
            
            return {
                'category_id': category_id,
                'category_name': category_name,
                'products_count': len(products) if products else 0,
                'status': 'success'
            }
            
        except Exception as e:
            print(f"  ⚠️  Lỗi khi crawl products từ {category_name}: {e}")
            return {
                'category_id': category_id,
                'category_name': category_name,
                'products_count': 0,
                'status': 'error',
                'error': str(e)
            }

    @task
    def aggregate_crawl_results(results: List[Dict[str, Any]]):
        """Tổng hợp kết quả crawl từ tất cả categories"""
        total_products = 0
        successful_categories = 0
        failed_categories = 0
        
        for result in results:
            if result.get('status') == 'success':
                successful_categories += 1
                total_products += result.get('products_count', 0)
            else:
                failed_categories += 1
        
        summary = {
            'total_categories': len(results),
            'successful_categories': successful_categories,
            'failed_categories': failed_categories,
            'total_products': total_products,
            'crawl_time': datetime.now().isoformat(),
            'results': results
        }
        
        # Lưu summary
        summary_file = os.path.join(
            config['data_paths']['processed'],
            f"crawl_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        os.makedirs(os.path.dirname(summary_file), exist_ok=True)
        
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        print(f"\n{'='*60}")
        print(f"Crawl Summary:")
        print(f"  - Tổng categories: {len(results)}")
        print(f"  - Thành công: {successful_categories}")
        print(f"  - Thất bại: {failed_categories}")
        print(f"  - Tổng products: {total_products}")
        print(f"  - Summary file: {summary_file}")
        print(f"{'='*60}")
        
        return summary

    # Task flow
    categories = load_categories_for_crawl()
    
    # Dynamic task mapping: mỗi category crawl song song
    crawl_results = crawl_products_from_category.expand(category=categories)
    
    # Tổng hợp kết quả
    summary = aggregate_crawl_results(crawl_results)
    
    # Dependencies
    categories >> crawl_results >> summary

