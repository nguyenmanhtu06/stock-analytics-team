import pandas as pd
from datetime import datetime, timedelta
from vnstock import Vnstock
from sqlalchemy import create_engine, text

# Thiết lập kết nối PostgreSQL
user = "postgres"
password = "postgres"
host = "localhost"
port = "5432"
database = "stockdb"
table_name = "stock_prices"

conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
engine = create_engine(conn_str)

# Hàm cập nhật dữ liệu cho 1 mã
def update_stock_price_nearest_to_postgres(symbol, table_name, engine):
    """Cập nhật dữ liệu cổ phiếu mới nhất cho 1 mã từ vnstock vào PostgreSQL."""
    try:
        query = text(f"SELECT * FROM {table_name} WHERE symbol = :symbol")
        df_old = pd.read_sql(query, engine, params={"symbol": symbol})

        if not df_old.empty and 'time' in df_old.columns:
            df_old['time'] = pd.to_datetime(df_old['time'])
            last_date = df_old['time'].max()
            print(f"📅 {symbol}: dữ liệu hiện có đến {last_date.date()}")
            start_date = (last_date + timedelta(days=1)).strftime('%Y-%m-%d')
        else:
            print(f"⚠️ {symbol}: chưa có dữ liệu trong bảng {table_name}, sẽ tải toàn bộ mới.")
            start_date = '2024-01-01'

        # Lấy dữ liệu mới nhất từ vnstock
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        df_latest_check = stock.quote.history(start='2024-01-01', end=datetime.today().strftime('%Y-%m-%d'))

        if df_latest_check.empty:
            print(f"❌ {symbol}: không lấy được dữ liệu từ vnstock.")
            return

        newest_date = df_latest_check['time'].max().date()
        print(f"🕒 {symbol}: ngày giao dịch mới nhất trên vnstock là {newest_date}")

        if not df_old.empty and last_date.date() >= newest_date:
            print(f"✅ {symbol}: dữ liệu đã cập nhật mới nhất (đến {last_date.date()}).")
            return

        # Lấy dữ liệu mới
        df_new = stock.quote.history(start=start_date, end=str(newest_date))

        if df_new.empty:
            print(f"✅ {symbol}: không có dữ liệu mới cần cập nhật.")
            return

        df_new['symbol'] = symbol

        # Loại bỏ dòng trùng (nếu có)
        if not df_old.empty:
            df_new = df_new[~df_new['time'].isin(df_old['time'])]

        if df_new.empty:
            print(f"✅ {symbol}: tất cả dữ liệu mới đã có trong bảng.")
            return

        # Ghi dữ liệu mới
        df_new.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"✅ {symbol}: đã thêm {len(df_new)} dòng mới (đến {df_new['time'].max().strftime('%Y-%m-%d')})")

    except Exception as e:
        print(f"❌ Lỗi khi cập nhật {symbol}: {e}")

# Chạy cập nhật cho 10 mã đầu
try:
    # Lấy danh sách 10 mã đầu tiên trong bảng
    df_symbols = pd.read_sql(f"SELECT DISTINCT symbol FROM {table_name} ORDER BY symbol ASC LIMIT 10", engine)
    symbols = df_symbols['symbol'].tolist()

    print(f"\n🚀 Bắt đầu cập nhật dữ liệu cho {len(symbols)} mã đầu tiên...")
    for symbol in symbols:
        update_stock_price_nearest_to_postgres(symbol, table_name, engine)

    print("\n🎯 Hoàn tất cập nhật tất cả mã.")
except Exception as e:
    print(f"❌ Lỗi tổng khi lấy danh sách mã: {e}")
