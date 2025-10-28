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

def get_all_symbols_today():
    """
    Lấy danh sách mã cổ phiếu đang được giao dịch tại thời điểm hiện tại (theo VCI).
    """
    try:
        vnstock_client = Vnstock()
        df_listed = vnstock_client.listing.symbols(source='VCI')
        symbols = df_listed['symbol'].dropna().unique().tolist()
        print(f"📈 Phát hiện {len(symbols)} mã cổ phiếu hiện có trên thị trường.")
        return symbols
    except Exception as e:
        raise RuntimeError(f"Lỗi khi quét danh sách mã mới: {e}")


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
        df_new = stock.quote.history(
            start=start_date,
            end=datetime.today().strftime('%Y-%m-%d')
        )

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
    """
    Hàm chính Airflow chạy:
    - Quét danh sách mã cổ phiếu mới nhất từ VCI.
    - So sánh với danh sách đã có trong database.
    - Thêm mới nếu có mã chưa có.
    - Cập nhật dữ liệu từng mã.
    """
    engine = create_engine(conn_str)

    # 1️⃣ Quét danh sách mã hiện có trên thị trường
    all_symbols = get_all_symbols_today()

    # 2️⃣ Lấy danh sách đã có trong DB
    try:
        df_existing = pd.read_sql(f"SELECT DISTINCT symbol FROM {table_name}", engine)
        existing_symbols = df_existing['symbol'].tolist()
    except Exception:
        existing_symbols = []
        print("⚠️ Bảng trống hoặc chưa tồn tại, sẽ tạo mới toàn bộ.")

    # 3️⃣ So sánh để tìm mã mới
    new_symbols = [s for s in all_symbols if s not in existing_symbols]
    print(f"🆕 Có {len(new_symbols)} mã mới cần thêm.")

    # 4️⃣ Danh sách cập nhật = tất cả (cũ + mới)
    all_to_update = sorted(set(existing_symbols + new_symbols))
    print(f"🚀 Tổng cộng {len(all_to_update)} mã sẽ được cập nhật.")

    # 5️⃣ Cập nhật từng mã
    for symbol in all_to_update:
        update_stock_price_nearest_to_postgres(symbol, table_name, engine)

    print("🎯 Hoàn tất cập nhật toàn bộ.")


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
    description="Extract & Load dữ liệu mỗi ngày (tự quét mã mới)",
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
