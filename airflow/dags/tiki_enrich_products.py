from datetime import datetime, timedelta

# Import Selenium crawling task from main package
from tiki_crawl_products_v2.tasks import crawl_product_batch_selenium

from airflow import DAG
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "tiki_enrich_products",
    default_args=default_args,
    description="Enrichment DAG: Crawl missing data (Brand, Specs) using Selenium",
    schedule=[
        Dataset("tiki-postgres://tiki_postgres/tiki/public/products")
    ],  # Run when dataset updates
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["tiki", "enrichment", "selenium"],
    max_active_runs=1,
)


def fetch_products_needing_enrichment(**context):
    """
    Fetch list of products flagged for enrichment (needs_enrichment=True)
    """
    postgres_hook = PostgresHook(postgres_conn_id="tiki_postgres")
    # Fetch random 100 products to process in this batch
    # Using random to avoid stuck items blocking others? Or just LIMIT is fine.
    # Let's take 200 items per run (to fit within execution time)
    records = postgres_hook.get_records("""
        SELECT product_id, url
        FROM products
        WHERE needs_enrichment = TRUE
          AND (crawl_quality != 'failed' OR crawl_quality IS NULL)
        ORDER BY updated_at ASC
        LIMIT 200
    """)
    if not records:
        print("No products need enrichment.")
        return []

    print(f"Found {len(records)} products needing enrichment.")
    # Convert to list of dicts
    products = [{"product_id": r[0], "url": r[1]} for r in records]
    return products


# enrich_product_batch removed (replaced by imported task)


def update_enriched_products(enriched_data, **context):
    """
    Update enriched data back to Database.
    """
    if not enriched_data:
        print("No data to update.")
        return

    postgres_hook = PostgresHook(postgres_conn_id="tiki_postgres")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    success_count = 0
    failed_count = 0

    for item in enriched_data:
        product_id = item["product_id"]
        status = item["status"]

        if status == "success" or status == "partial":
            detail = item.get("detail", {})

            # Update query
            # We only update fields that Selenium is good at fetching: Brand, Specs, maybe Seller
            # And specific fields found in detail

            brand = detail.get("brand", "Unknown")
            seller_name = detail.get("seller", {}).get("name")

            try:
                cursor.execute(
                    """
                    UPDATE products
                    SET
                        brand = %s,
                        seller_name = COALESCE(%s, seller_name),
                        crawl_quality = 'full',
                        needs_enrichment = FALSE,
                        updated_at = NOW()
                    WHERE product_id = %s
                """,
                    (brand, seller_name, product_id),
                )
                success_count += 1
            except Exception as e:
                print(f"Error updating DB for {product_id}: {e}")
                conn.rollback()
                continue

        else:
            # If failed, increment retry or mark as permanent fail?
            # For now, just leave needs_enrichment=TRUE so it picks up again?
            # Or mark crawl_quality='failed' to avoid infinite loop
            # Let's mark as 'failed' if it failed selenium crawl
            try:
                cursor.execute(
                    """
                    UPDATE products
                    SET crawl_quality = 'failed', updated_at = NOW()
                    WHERE product_id = %s
                """,
                    (product_id,),
                )
                failed_count += 1
            except Exception:
                pass

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Updated {success_count} products. Failed {failed_count}.")


# Define Tasks
fetch_task = PythonOperator(
    task_id="fetch_products_needing_enrichment",
    python_callable=fetch_products_needing_enrichment,
    dag=dag,
)


def _enrich_task_wrapper(**context):
    ti = context["ti"]
    products = ti.xcom_pull(task_ids="fetch_products_needing_enrichment")
    # Call imported batch crawling function
    # Note: crawl_product_batch_selenium expects 'product_batch' argument or kwargs
    return crawl_product_batch_selenium(product_batch=products, batch_index=0, **context)


enrich_task = PythonOperator(
    task_id="enrich_product_batch",
    python_callable=_enrich_task_wrapper,
    dag=dag,
    execution_timeout=timedelta(minutes=60),  # Allow time for Selenium
)


def _update_task_wrapper(**context):
    ti = context["ti"]
    results = ti.xcom_pull(task_ids="enrich_product_batch")
    update_enriched_products(results, **context)


update_task = PythonOperator(
    task_id="update_enriched_products",
    python_callable=_update_task_wrapper,
    dag=dag,
)

fetch_task >> enrich_task >> update_task
