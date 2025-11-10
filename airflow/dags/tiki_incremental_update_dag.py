"""
DAG để incremental update - chỉ crawl những gì thay đổi
Tối ưu cho việc cập nhật dữ liệu thường xuyên
"""
from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import sys
import os
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../src'))

from pipelines.crawl.tiki.config import get_config

config = get_config()

default_args = {
    'owner': 'tiki-data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    'tiki_incremental_update',
    default_args=default_args,
    description='Incremental update cho Tiki data - chỉ crawl những gì thay đổi',
    schedule=timedelta(hours=6),  # Chạy mỗi 6 giờ
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['tiki', 'incremental', 'update'],
    max_active_runs=1,
) as dag:

    @task
    def check_for_updates():
        """
        Kiểm tra xem có categories/products mới cần crawl không
        So sánh với lần crawl trước
        """
        # Load categories hiện tại
        categories_file = config['data_paths']['all_categories']
        
        if not os.path.exists(categories_file):
            print("Chưa có file categories, cần crawl đầy đủ")
            return {'needs_full_crawl': True, 'new_categories': []}
        
        with open(categories_file, 'r', encoding='utf-8') as f:
            current_categories = json.load(f)
        
        # Load metadata từ lần crawl trước
        metadata_file = os.path.join(
            config['data_paths']['processed'],
            'crawl_metadata.json'
        )
        
        if not os.path.exists(metadata_file):
            print("Chưa có metadata, cần crawl đầy đủ")
            return {
                'needs_full_crawl': True,
                'new_categories': current_categories[:10]  # Crawl 10 đầu tiên
            }
        
        with open(metadata_file, 'r', encoding='utf-8') as f:
            last_metadata = json.load(f)
        
        # So sánh để tìm categories mới hoặc thay đổi
        last_category_ids = set(
            cat.get('category_id') for cat in last_metadata.get('categories', [])
            if cat.get('category_id')
        )
        
        current_category_ids = set(
            cat.get('category_id') for cat in current_categories
            if cat.get('category_id')
        )
        
        new_category_ids = current_category_ids - last_category_ids
        
        new_categories = [
            cat for cat in current_categories
            if cat.get('category_id') in new_category_ids
        ]
        
        print(f"Tìm thấy {len(new_categories)} categories mới")
        
        return {
            'needs_full_crawl': False,
            'new_categories': new_categories,
            'new_count': len(new_categories)
        }

    @task
    def update_categories(update_info):
        """Cập nhật categories mới"""
        if update_info.get('needs_full_crawl'):
            print("Cần crawl đầy đủ - trigger DAG crawl_categories")
            # Có thể trigger DAG khác ở đây
            return {'status': 'full_crawl_needed'}
        
        new_categories = update_info.get('new_categories', [])
        
        if not new_categories:
            print("Không có categories mới")
            return {'status': 'no_updates'}
        
        # Crawl sub-categories cho các categories mới
        from pipelines.crawl.tiki.extract_category_link import crawl_sub_categories
        
        all_new_sub_categories = []
        
        for category in new_categories:
            cat_name = category.get('name', 'N/A')
            cat_url = category.get('url', '')
            cat_id = category.get('category_id', '')
            
            print(f"Đang crawl sub-categories của category mới: {cat_name}")
            
            try:
                sub_cats = crawl_sub_categories(
                    category_url=cat_url,
                    parent_category_id=cat_id,
                    parent_name=cat_name
                )
                
                if sub_cats:
                    all_new_sub_categories.extend(sub_cats)
                    print(f"  ✓ Tìm thấy {len(sub_cats)} sub-categories")
            except Exception as e:
                print(f"  ⚠️  Lỗi: {e}")
                continue
        
        # Merge vào file all_categories
        if all_new_sub_categories:
            all_categories_file = config['data_paths']['all_categories']
            
            with open(all_categories_file, 'r', encoding='utf-8') as f:
                existing_categories = json.load(f)
            
            # Thêm categories mới
            existing_categories.extend(all_new_sub_categories)
            
            # Remove duplicates
            seen_ids = set()
            unique_categories = []
            for cat in existing_categories:
                cat_id = cat.get('category_id')
                if cat_id and cat_id not in seen_ids:
                    seen_ids.add(cat_id)
                    unique_categories.append(cat)
            
            # Lưu lại
            with open(all_categories_file, 'w', encoding='utf-8') as f:
                json.dump(unique_categories, f, indent=2, ensure_ascii=False)
            
            print(f"Đã cập nhật {len(all_new_sub_categories)} sub-categories mới")
        
        return {
            'status': 'updated',
            'new_sub_categories_count': len(all_new_sub_categories)
        }

    @task
    def save_metadata(update_result):
        """Lưu metadata của lần crawl này"""
        categories_file = config['data_paths']['all_categories']
        
        if os.path.exists(categories_file):
            with open(categories_file, 'r', encoding='utf-8') as f:
                categories = json.load(f)
        else:
            categories = []
        
        metadata = {
            'last_update': datetime.now().isoformat(),
            'categories': categories[:100],  # Chỉ lưu 100 đầu để tiết kiệm
            'total_categories': len(categories),
            'update_result': update_result
        }
        
        metadata_file = os.path.join(
            config['data_paths']['processed'],
            'crawl_metadata.json'
        )
        os.makedirs(os.path.dirname(metadata_file), exist_ok=True)
        
        with open(metadata_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        
        print(f"Đã lưu metadata vào: {metadata_file}")

    # Task flow
    update_info = check_for_updates()
    update_result = update_categories(update_info)
    save_metadata(update_result)
    
    # Dependencies
    update_info >> update_result >> save_metadata

