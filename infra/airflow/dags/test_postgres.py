from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import psycopg2

# Connection settings (match your docker-compose/.env)
PG_HOST = "postgres"
PG_PORT = 5432
PG_USER = "admin"
PG_PASSWORD = "admin123"
PG_DB = "vnstock"

def insert_test_row():
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        user=PG_USER,
        password=PG_PASSWORD,
        dbname=PG_DB
    )
    cur = conn.cursor()

    # Create a test table if it doesn't exist
    cur.execute("""
        CREATE TABLE IF NOT EXISTS airflow_test (
            id SERIAL PRIMARY KEY,
            message TEXT,
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)

    # Insert one test row
    cur.execute("INSERT INTO airflow_test (message) VALUES (%s)", ("Hello from Airflow!",))

    conn.commit()
    cur.close()
    conn.close()

default_args = {"owner": "airflow", "start_date": datetime(2025, 1, 1)}

with DAG(
    dag_id="test_postgres_dag",
    default_args=default_args,
    schedule_interval=None,  # run manually
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="insert_test_row",
        python_callable=insert_test_row
    )
