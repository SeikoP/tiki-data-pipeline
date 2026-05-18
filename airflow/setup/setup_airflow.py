from airflow.models import Connection, Pool, Variable
from airflow.utils.session import create_session
from common.settings import POOL_DEFAULTS, airflow_variable_defaults, get_pipeline_settings


def setup_airflow():
    print("🚀 Starting optimized Airflow configuration...")

    with create_session() as session:
        # 1. Setup Pools
        pools_data = [
            (name, data["slots"], data["description"]) for name, data in POOL_DEFAULTS.items()
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
        variables_data = airflow_variable_defaults()

        for key, val in variables_data.items():
            Variable.set(key, val, session=session)
            print(f"   Set variable: {key}")

        # 3. Setup Connections
        conn_id = "tiki_postgres"
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()

        db_settings = get_pipeline_settings(use_airflow=False).database
        postgres_host = db_settings.host
        postgres_user = db_settings.user
        postgres_pass = db_settings.password
        postgres_port = db_settings.port
        postgres_db = db_settings.database
        if not postgres_pass:
            print("   Warning: POSTGRES_PASSWORD is empty; connection will use empty password")

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
