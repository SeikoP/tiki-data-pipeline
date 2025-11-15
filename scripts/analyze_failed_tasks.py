"""
Script để phân tích các task failed trong DAG
Giúp hiểu rõ nguyên nhân và loại lỗi
"""
import json
import os
from collections import defaultdict
from pathlib import Path


def analyze_failed_tasks_from_merge_result(merge_result_file=None):
    """
    Phân tích các task failed từ merge result hoặc cache files

    Args:
        merge_result_file: Đường dẫn file merge result (nếu có)
    """
    print("="*70)
    print("PHÂN TÍCH CÁC TASK FAILED")
    print("="*70)

    # Tìm merge result file
    if not merge_result_file:
        possible_files = [
            'data/raw/products/products_with_detail.json',
            'data/raw/products/products_batch_0.json',
            'data/raw/products/products_batch_1.json'
        ]
        for file_path in possible_files:
            if os.path.exists(file_path):
                merge_result_file = file_path
                break

    failed_analysis = {
        'total_products': 0,
        'success_count': 0,
        'failed_count': 0,
        'error_types': defaultdict(int),
        'error_messages': defaultdict(int),
        'failed_products': []
    }

    # Phân tích từ merge result
    if merge_result_file and os.path.exists(merge_result_file):
        print(f"\nDang phan tich file: {merge_result_file}")
        try:
            with open(merge_result_file, encoding='utf-8') as f:
                data = json.load(f)

            products = data.get('products', [])
            failed_analysis['total_products'] = len(products)

            for product in products:
                # Kiểm tra status từ detail_status hoặc error
                detail_status = product.get('detail_status')
                error = product.get('error')
                has_detail = product.get('detail') is not None or product.get('price') is not None

                if detail_status == 'failed' or (not has_detail and error):
                    failed_analysis['failed_count'] += 1

                    # Phân loại lỗi
                    error_msg = error or product.get('detail_error', 'Unknown error')
                    error_type = 'unknown'

                    if 'timeout' in error_msg.lower():
                        error_type = 'timeout'
                    elif 'selenium' in error_msg.lower() or 'chrome' in error_msg.lower() or 'driver' in error_msg.lower():
                        error_type = 'selenium_error'
                    elif 'network' in error_msg.lower() or 'connection' in error_msg.lower():
                        error_type = 'network_error'
                    elif 'memory' in error_msg.lower():
                        error_type = 'memory_error'
                    elif 'extract' in error_msg.lower():
                        error_type = 'extract_error'
                    elif 'validation' in error_msg.lower():
                        error_type = 'validation_error'

                    failed_analysis['error_types'][error_type] += 1
                    failed_analysis['error_messages'][error_msg[:100]] += 1

                    failed_analysis['failed_products'].append({
                        'product_id': product.get('product_id'),
                        'url': product.get('url', '')[:60],
                        'error': error_msg[:200],
                        'error_type': error_type,
                        'status': detail_status
                    })
                else:
                    failed_analysis['success_count'] += 1

        except Exception as e:
            print(f"❌ Lỗi khi đọc file: {e}")

    # Phân tích từ cache files (nếu có)
    cache_dir = Path('data/raw/products/detail/cache')
    if cache_dir.exists():
        print(f"\nDang phan tich cache files trong: {cache_dir}")
        cache_files = list(cache_dir.glob('*.json'))
        print(f"   Tim thay {len(cache_files)} cache files")

    # In kết quả
    print("\nTHONG KE")
    print(f"{'='*70}")
    print(f"Tong so products: {failed_analysis['total_products']}")
    print(f"Success: {failed_analysis['success_count']} ({failed_analysis['success_count']/failed_analysis['total_products']*100:.1f}%)" if failed_analysis['total_products'] > 0 else "Success: 0")
    print(f"Failed: {failed_analysis['failed_count']} ({failed_analysis['failed_count']/failed_analysis['total_products']*100:.1f}%)" if failed_analysis['total_products'] > 0 else "Failed: 0")

    if failed_analysis['error_types']:
        print("\nPHAN LOAI LOI:")
        for error_type, count in sorted(failed_analysis['error_types'].items(), key=lambda x: x[1], reverse=True):
            print(f"   - {error_type}: {count} ({count/failed_analysis['failed_count']*100:.1f}%)" if failed_analysis['failed_count'] > 0 else f"   - {error_type}: {count}")

    if failed_analysis['error_messages']:
        print("\nTOP ERROR MESSAGES:")
        for error_msg, count in sorted(failed_analysis['error_messages'].items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"   - {error_msg[:80]}...: {count}")

    if failed_analysis['failed_products']:
        print("\nVI DU FAILED PRODUCTS (10 dau tien):")
        for i, product in enumerate(failed_analysis['failed_products'][:10], 1):
            print(f"   {i}. Product ID: {product['product_id']}")
            print(f"      URL: {product['url']}...")
            print(f"      Error Type: {product['error_type']}")
            print(f"      Error: {product['error'][:100]}...")
            print()

    # Lưu kết quả
    output_file = 'data/test_output/failed_tasks_analysis.json'
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(failed_analysis, f, ensure_ascii=False, indent=2)

    print(f"\nDa luu phan tich vao: {output_file}")
    print(f"{'='*70}")

    return failed_analysis


def analyze_from_airflow_logs():
    """
    Hướng dẫn phân tích từ Airflow logs
    """
    print("\n" + "="*70)
    print("HUONG DAN PHAN TICH TU AIRFLOW LOGS")
    print("="*70)
    print("""
De xem chi tiet cac task failed trong Airflow:

1. Vao Airflow UI -> DAGs -> tiki_crawl_products
2. Click vao DAG Run gan nhat
3. Xem Tree View hoac Graph View
4. Cac task mau do la failed tasks
5. Click vao tung failed task de xem:
   - Logs: Xem error message chi tiet
   - Task Instance Details: Xem thong tin task
   - XCom: Xem du lieu tra ve

Cac loai loi pho bien:
- selenium_error: Loi Selenium/Chrome driver
- timeout: Timeout khi crawl
- network_error: Loi mang
- memory_error: Het memory
- extract_error: Loi khi extract data
- validation_error: Loi validation (URL khong hop le, etc.)

De xem logs tu command line:
  airflow tasks logs <dag_id> <task_id> <execution_date>
    """)


def main():
    """Chạy phân tích"""
    # Phân tích từ merge result
    analysis = analyze_failed_tasks_from_merge_result()

    # Hướng dẫn phân tích từ Airflow
    analyze_from_airflow_logs()

    return analysis


if __name__ == "__main__":
    main()

