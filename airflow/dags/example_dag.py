"""
Example Airflow DAG - Template để tạo DAG mới

Đây là một DAG mẫu để bạn có thể tham khảo và tạo DAG mới.
"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

# Default arguments cho DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Định nghĩa DAG
with DAG(
    'example_dag',
    default_args=default_args,
    description='Example DAG template',
    schedule=timedelta(days=1),  # Chạy hàng ngày (Airflow 3.x dùng 'schedule' thay vì 'schedule_interval')
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Không chạy lại các task đã bỏ lỡ
    tags=['example', 'template'],
) as dag:

    # Task 1: Hello World
    def hello_world():
        """Task đơn giản in ra Hello World"""
        print("Hello from Airflow!")
        return "Hello World"

    task_hello = PythonOperator(
        task_id='hello_world',
        python_callable=hello_world,
    )

    # Task 2: Bash command
    task_bash = BashOperator(
        task_id='bash_task',
        bash_command='echo "This is a bash task"',
    )

    # Task 3: Python với dependencies
    def process_data():
        """Task xử lý dữ liệu"""
        import requests
        
        # Ví dụ: Gọi Firecrawl API
        # response = requests.post(
        #     'http://api:3002/v0/scrape',
        #     json={'url': 'https://example.com'},
        #     headers={'Authorization': f'Bearer {TEST_API_KEY}'}
        # )
        # return response.json()
        
        print("Processing data...")
        return "Data processed"

    task_process = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
    )

    # Định nghĩa dependencies (task flow)
    task_hello >> task_bash >> task_process

