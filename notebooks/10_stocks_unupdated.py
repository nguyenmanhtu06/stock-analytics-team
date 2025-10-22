# pip install -U vnstock
# pip install psycopg2-binary

from vnstock.explorer.vci import Listing
import pandas as pd
from tqdm import tqdm  # hiển thị tiến trình
from vnstock import Vnstock
from sqlalchemy import create_engine

# Kết nối tới PostgreSQL local
user = "postgres"
password = "postgres"
host = "localhost"
port = "5432"
database = "stockdb"

engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}")

# Lấy toàn bộ danh sách mã cổ phiếu
listing = Listing().all_symbols()
print(f"Tổng số mã cổ phiếu: {len(listing)}")

# Sắp xếp theo symbol A → Z
listing = listing.sort_values(by='symbol', ascending=True).reset_index(drop=True)
print("10 mã cổ phiếu đầu tiên theo thứ tự alphabet:")
print(listing['symbol'].head(10).tolist())

# Lấy dữ liệu lịch sử giá cho 10 mã đầu tiên
symbols = listing['symbol'].tolist()
all_data = pd.DataFrame()

for symbol in tqdm(symbols[:10]):
    try:
        stock = Vnstock().stock(symbol=symbol, source='VCI')
        df = stock.quote.history(start='2024-01-01', end='2025-10-10') #Tạo bảng dữ liệu chưa update để áp hàm update

        if not df.empty:
            df['symbol'] = symbol  # thêm mã cổ phiếu tương ứng
            print(f"✅ {symbol}: có dữ liệu đến ngày {df['time'].max().strftime('%Y-%m-%d')}")
            
            # Append vào DataFrame tổng
            all_data = pd.concat([all_data, df], ignore_index=True)
        else:
            print(f"⚠️ {symbol}: không có dữ liệu.")
    except Exception as e:
        print(f"❌ Lỗi khi cập nhật {symbol}: {e}")

# Ghi toàn bộ dữ liệu vào PostgreSQL
if not all_data.empty:
    table_name = "stock_prices"
    all_data.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"\n✅ Đã ghi {len(all_data)} dòng vào bảng '{table_name}' trong database '{database}'.")
else:
    print("\n⚠️ Không có dữ liệu hợp lệ để ghi vào database.")