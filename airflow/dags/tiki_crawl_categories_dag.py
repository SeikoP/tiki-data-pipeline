"""
DAG để crawl categories từ Tiki với dynamic task mapping
Tối ưu cho crawl dữ liệu khổng lồ với parallel processing
"""
from airflow import DAG
try:
    from airflow.decorators import task, task_group
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup
except ImportError:
    # Fallback cho Airflow versions khác nhau
    from airflow.operators.python import PythonOperator
    from airflow.utils.task_group import TaskGroup
    # task decorator sẽ được định nghĩa lại nếu cần
from datetime import datetime, timedelta
import sys
import os

# Thêm path để import modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from pipelines.crawl.tiki.extract_category_link import (
    load_categories_from_json,
    crawl_sub_categories,
    crawl_all_sub_categories,
    create_merged_categories_file
)
from pipelines.crawl.tiki.config import get_config

config = get_config()

default_args = {
    'owner': 'tiki-data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    'tiki_crawl_categories',
    default_args=default_args,
    description='Crawl categories từ Tiki.vn với parallel processing',
    schedule=timedelta(days=7),  # Chạy mỗi tuần
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tiki', 'crawl', 'categories'],
    max_active_runs=1,  # Chỉ chạy 1 instance tại một thời điểm
) as dag:

    @task
    def load_parent_categories():
        """Load parent categories từ file hoặc crawl mới"""
        categories_file = config['data_paths']['categories']
        
        if os.path.exists(categories_file):
            print(f"Đang load categories từ {categories_file}")
            categories = load_categories_from_json(categories_file)
            print(f"Đã load {len(categories)} categories")
            return categories
        else:
            print("File categories chưa tồn tại, cần crawl mới")
            # Có thể gọi function crawl từ extract_category_link.py
            return []

    @task
    def crawl_category_batch(category_batch):
        """
        Crawl sub-categories cho một batch categories
        Sử dụng dynamic task mapping để chạy song song
        """
        results = []
        for category in category_batch:
            cat_name = category.get('name', 'N/A')
            cat_url = category.get('url', '')
            cat_id = category.get('category_id', '')
            
            print(f"Đang crawl sub-categories của: {cat_name} (ID: {cat_id})")
            
            try:
                sub_cats = crawl_sub_categories(
                    category_url=cat_url,
                    parent_category_id=cat_id,
                    parent_name=cat_name
                )
                
                if sub_cats:
                    print(f"  ✓ Tìm thấy {len(sub_cats)} sub-categories")
                    results.extend(sub_cats)
                else:
                    print(f"  - Không tìm thấy sub-categories")
                    
            except Exception as e:
                print(f"  ⚠️  Lỗi khi crawl {cat_name}: {e}")
                # Không raise để các batch khác vẫn chạy được
                continue
        
        return results

    @task
    def merge_all_sub_categories(sub_categories_list):
        """Merge tất cả sub-categories từ các batches"""
        all_sub_categories = []
        
        for batch_results in sub_categories_list:
            if batch_results:
                all_sub_categories.extend(batch_results)
        
        # Remove duplicates
        seen_ids = set()
        unique_sub_categories = []
        
        for sub_cat in all_sub_categories:
            cat_id = sub_cat.get('category_id')
            if cat_id and cat_id not in seen_ids:
                seen_ids.add(cat_id)
                unique_sub_categories.append(sub_cat)
        
        print(f"Tổng cộng {len(unique_sub_categories)} unique sub-categories")
        
        # Lưu vào file
        output_file = config['data_paths']['sub_categories']
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        import json
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(unique_sub_categories, f, indent=2, ensure_ascii=False)
        
        print(f"Đã lưu vào: {output_file}")
        
        return unique_sub_categories

    @task
    def create_merged_file():
        """Tạo file JSON hợp nhất với cấu trúc phân cấp"""
        merged_file = create_merged_categories_file()
        return merged_file

    # Task flow
    parent_categories = load_parent_categories()
    
    # Chia categories thành batches để xử lý song song
    batch_size = config['crawl']['batch_size']
    
    @task
    def create_batches(categories):
        """Chia categories thành batches"""
        batches = []
        for i in range(0, len(categories), batch_size):
            batches.append(categories[i:i + batch_size])
        print(f"Đã chia thành {len(batches)} batches (mỗi batch {batch_size} categories)")
        return batches
    
    batches = create_batches(parent_categories)
    
    # Dynamic task mapping: mỗi batch chạy song song
    sub_categories_results = crawl_category_batch.expand(category_batch=batches)
    
    # Merge kết quả
    all_sub_categories = merge_all_sub_categories(sub_categories_results)
    
    # Tạo file hợp nhất
    merged_file = create_merged_file()
    
    # Dependencies
    parent_categories >> batches >> sub_categories_results >> all_sub_categories >> merged_file

