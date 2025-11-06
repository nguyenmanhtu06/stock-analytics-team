from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
from vnstock import Listing, Company
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def get_value_safe(data, key):
    val = data.get(key, None)
    if isinstance(val, pd.Series):
        return val.iloc[0]
    elif isinstance(val, list):
        return val[0] if len(val) > 0 else None
    return val

def get_batches(lst, batch_size):
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]

def get_dim_stock_batch(symbols_batch, symbol_to_organ_name):
    data_rows = []
    for sym in symbols_batch:
        organ_name = symbol_to_organ_name.get(sym, None)

        try:
            tcb_company = Company(symbol=sym, source='TCBS').overview()
            tcb_data = {
                'symbol': sym,
                'exchange': get_value_safe(tcb_company, 'exchange'),
                'organ_name': organ_name
            }
        except Exception:
            tcb_data = {'symbol': sym, 'exchange': None, 'organ_name': organ_name}

        try:
            time.sleep(2)  # limit rate
            vci_company = Company(symbol=sym, source='VCI').overview()
            vci_data = {
                'icb_name2': get_value_safe(vci_company, 'icb_name2'),
                'icb_name3': get_value_safe(vci_company, 'icb_name3'),
                'icb_name4': get_value_safe(vci_company, 'icb_name4')
            }
        except Exception:
            vci_data = {'icb_name2': None, 'icb_name3': None, 'icb_name4': None}

        combined = {**tcb_data, **vci_data}
        data_rows.append(combined)
    return data_rows

def get_dim_stock_combined_with_batches(save_path='/opt/airflow/data/dim_stock.csv', batch_size=50, **kwargs):
    listing = Listing()
    symbols_df = listing.all_symbols().sort_values(by='symbol').reset_index(drop=True)

    symbols = symbols_df['symbol'].tolist()
    symbol_to_organ_name = dict(zip(symbols_df['symbol'], symbols_df['organ_name']))

    all_data_rows = []

    for batch_num, batch_symbols in enumerate(get_batches(symbols, batch_size), start=1):
        batch_rows = get_dim_stock_batch(batch_symbols, symbol_to_organ_name)
        all_data_rows.extend(batch_rows)

        print(f"Đã xử lý batch {batch_num} gồm {len(batch_symbols)} mã, tổng dữ liệu hiện tại: {len(all_data_rows)}")

        time.sleep(10)  # nghỉ giữa các batch

    dim_stock = pd.DataFrame(all_data_rows)
    dim_stock.reset_index(drop=True, inplace=True)
    dim_stock.insert(0, 'stock_id', dim_stock.index + 1)

    dim_stock.to_csv(save_path, index=False, encoding='utf-8-sig')
    print(f"Đã lưu file CSV: {save_path}")

with DAG(
    'vnstock_dim_stock_weekly', 
    default_args=default_args,
    description='Weekly update dim_stock data from vnstock',
    schedule_interval='@weekly',
    start_date=datetime(2025, 11, 6),
    catchup=False,
    max_active_runs=1
) as dag:
    run_dim_stock_update = PythonOperator(
        task_id='get_dim_stock_data',
        python_callable=get_dim_stock_combined_with_batches,
        op_kwargs={'save_path': '/opt/airflow/data/dim_stock.csv', 'batch_size': 50}
    )