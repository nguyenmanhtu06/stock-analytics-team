from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from vnstock import Vnstock
from sqlalchemy import create_engine, text

# ======================
# CONFIG
# ======================
user = "postgres"
password = "postgres"
host = "postgres"
port = "5432"
database = "stockdb"
table_name = "once_time_stock"

conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"

# ======================
# FUNCTION
# ======================
def update_stock_price_nearest_to_postgres(symbol, table_name, engine):
    """Cập nhật dữ liệu cổ phiếu mới nhất cho 1 mã từ vnstock vào PostgreSQL."""
    try:
        query = text(f"SELECT * FROM {table_name} WHERE symbol = :symbol")
        df_old = pd.read_sql(query, engine, params={"symbol": symbol})

        if not df_old.empty and 'time' in df_old.columns:
            df_old['time'] = pd.to_datetime(df_old['time'])
            last_date = df_old['time'].max()
            start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            start_date = '2024-01-01'

        stock = Vnstock().stock(symbol=symbol, source='VCI')
        df_new = stock.quote.history(start=start_date, end=datetime.today().strftime('%Y-%m-%d'))

        if df_new.empty:
            print(f"✅ {symbol}: không có dữ liệu mới.")
            return

        df_new['symbol'] = symbol
        df_new.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"✅ {symbol}: đã thêm {len(df_new)} dòng mới.")

    except Exception as e:
        print(f"⚠️ Bỏ qua {symbol}: {e}")
        return


def run_update_all_symbols():
    """Hàm chính Airflow sẽ chạy."""
    engine = create_engine(conn_str)

    try:
        df_symbols = pd.read_sql(
            f"SELECT DISTINCT symbol FROM {table_name}", engine
        )
        symbols = df_symbols['symbol'].tolist()
        print(f"🚀 Cập nhật {len(symbols)} mã...")

        for symbol in symbols:
            update_stock_price_nearest_to_postgres(symbol, table_name, engine)

        print("🎯 Hoàn tất cập nhật.")
    except Exception as e:
        raise RuntimeError(f"Lỗi tổng: {e}")


# ======================
# DAG DEFINITION
# ======================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    dag_id="el_daily_update_once_time_stock",
    default_args=default_args,
    description="Extract & Load dữ liệu mỗi ngày",
    schedule_interval="@daily",
    start_date=datetime(2025, 10, 23),
    catchup=False,
    tags=["EL", "daily"],
) as dag:

    update_data = PythonOperator(
        task_id="extract_and_load_data",
        python_callable=run_update_all_symbols,
    )

    update_data
