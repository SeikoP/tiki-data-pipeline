import os

from airflow.models import Connection, Pool, Variable
from airflow.utils.session import create_session


def setup_airflow():
    print("🚀 Starting optimized Airflow configuration...")

    with create_session() as session:
        # 1. Setup Pools
        pools_data = [
            ("fast_pool", 6, "Pool for Fast API and Data Processing tasks"),
            ("crawl_pool", 3, "Pool for Tiki crawling tasks (Selenium)"),
        ]

        for name, slots, desc in pools_data:
            pool = session.query(Pool).filter(Pool.pool == name).first()
            if not pool:
                print(f"   Creating pool: {name}")
                session.add(Pool(pool=name, slots=slots, description=desc, include_deferred=False))
            else:
                print(f"   Updating pool: {name}")
                pool.slots = slots
                pool.description = desc
                pool.include_deferred = False

        # 2. Setup Variables
        variables_data = {
            "TIKI_MIN_CATEGORY_LEVEL": "2",
            "TIKI_MAX_CATEGORY_LEVEL": "4",
            "TIKI_MAX_CATEGORIES": "0",
            "TIKI_DAG_SCHEDULE_MODE": "manual",
            "TIKI_SAVE_BATCH_SIZE": "100",
            "TIKI_CRAWL_BATCH_SIZE": "5",
            "TIKI_CATEGORY_NUM_BATCHES": "3",
            "TIKI_CATEGORY_CONCURRENCY": "6",
            "TIKI_DETAIL_BATCH_SIZE": "333",
            "TIKI_DETAIL_MAX_CONCURRENT_TASKS": "6",
            "TIKI_PRODUCTS_PER_DAY": "3000",  # Crawl 3000 products/run để crawl hết trong 1 ngày
            "TIKI_CACHE_RELAX_DAYS": "2",  # Update mỗi 2 ngày để track price changes tốt
            "TIKI_CIRCUIT_BREAKER_FAILURE_THRESHOLD": "5",
            "TIKI_CIRCUIT_BREAKER_RECOVERY_TIMEOUT": "60",
            "TIKI_DEGRADATION_FAILURE_THRESHOLD": "3",
            "TIKI_DEGRADATION_RECOVERY_THRESHOLD": "5",
            "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "postgres"),
            "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
            "POSTGRES_DB": os.environ.get("POSTGRES_DB", "tiki"),
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", "airflow_user"),
            "REDIS_URL": "redis://redis:6379/1",
        }

        for key, val in variables_data.items():
            Variable.set(key, val, session=session)
            print(f"   Set variable: {key}")

        # 3. Setup Connections
        conn_id = "tiki_postgres"
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

        postgres_host = os.environ.get("POSTGRES_HOST", "postgres")
        postgres_user = os.environ.get("POSTGRES_USER", "admin")
        postgres_pass = os.environ.get("POSTGRES_PASSWORD", "admin")
        postgres_port = int(os.environ.get("POSTGRES_PORT", "5432"))
        postgres_db = os.environ.get("POSTGRES_DB", "tiki")

        if not conn:
            print(f"   Creating connection: {conn_id}")
            new_conn = Connection(
                conn_id=conn_id,
                conn_type="postgres",
                host=postgres_host,
                login=postgres_user,
                password=postgres_pass,
                port=postgres_port,
                schema=postgres_db,
            )
            session.add(new_conn)
        else:
            print(f"   Updating connection: {conn_id}")
            conn.host = postgres_host
            conn.login = postgres_user
            conn.password = postgres_pass
            conn.port = postgres_port
            conn.schema = postgres_db

        session.commit()

    print("✅ Optimized Airflow configuration completed!")


if __name__ == "__main__":
    setup_airflow()
