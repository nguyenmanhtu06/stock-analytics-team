from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
from vnstock import Company
import pandas as pd
import warnings
import re
import os
from sqlalchemy import create_engine

warnings.simplefilter(action='ignore', category=FutureWarning)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# ==== Đọc config DB từ ENV ====
postgres_host = os.getenv("POSTGRES_HOST", "postgres_data")
postgres_port = os.getenv("POSTGRES_PORT", "5432")
postgres_user = os.getenv("POSTGRES_USER", "admin")
postgres_password = os.getenv("POSTGRES_PASSWORD", "admin123")
postgres_db = os.getenv("POSTGRES_DB", "vnstock")

conn_str = f"postgresql+psycopg2://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
table_name = "dim_company"

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

def format_history(history):
    if history is None or pd.isna(history):
        return None
    history_str = str(history).strip()
    if '.' in history_str or '!' in history_str:
        history_str = re.sub(r'([.!])\s*(-\s*)', r'\1\n-', history_str)
    else:
        history_str = history_str.replace('- ', '\n- ')
    lines = history_str.split('\n')
    lines_clean = [line.lstrip() for line in lines]
    return '\n'.join(lines_clean).strip()

def format_capital(charter_capital):
    if charter_capital is None or pd.isna(charter_capital):
        return None
    try:
        return int(float(str(charter_capital).replace(',', '').strip()))
    except Exception:
        return charter_capital

def get_dim_company_batch(batch_symbols, symbol_to_profile, stockid_dict):
    batch_rows = []
    for symbol in batch_symbols:
        stock_id = stockid_dict.get(symbol, None)
        company_profile = symbol_to_profile.get(symbol, None)
        try:
            tcb_company = Company(symbol=symbol, source='TCBS').overview()
            website = get_value_safe(tcb_company, 'website')
            if isinstance(website, (list, pd.Series)):
                website = website[0] if len(website) > 0 else None
            if website is not None:
                website = str(website).strip()
        except Exception:
            website = None
        try:
            time.sleep(2)
            vci_company = Company(symbol=symbol, source='VCI').overview()
            vci_internal_id = get_value_safe(vci_company, 'id')
            if isinstance(vci_internal_id, (list, pd.Series)):
                vci_internal_id = vci_internal_id[0] if len(vci_internal_id) > 0 else None
            if vci_internal_id is not None:
                vci_internal_id = str(vci_internal_id).strip()
            history = get_value_safe(vci_company, 'history')
            charter_capital = get_value_safe(vci_company, 'charter_capital')
        except Exception:
            history = None
            charter_capital = None
            vci_internal_id = None
        history_formatted = format_history(history)
        capital_formatted = format_capital(charter_capital)
        batch_rows.append({
            'company_id': None,  # sẽ cập nhật sau toàn bộ batch
            'stock_id': stock_id,
            'company_profile': company_profile,
            'history': history_formatted,
            'charter_capital': capital_formatted,
            'website': website,
            'vci_internal_id': vci_internal_id,
        })
    return batch_rows

def get_dim_company_from_stock(
    dim_stock_path='/opt/airflow/data/dim_stock.csv',
    batch_size=50,
    **kwargs
):
    df_stock = pd.read_csv(dim_stock_path, encoding='utf-8-sig')
    company_rows = []
    symbol_to_profile = dict(zip(df_stock['symbol'], df_stock['organ_name']))
    symbols = df_stock['symbol'].tolist()
    stockid_dict = dict(zip(df_stock['symbol'], df_stock['stock_id']))

    for batch_num, batch_symbols in enumerate(get_batches(symbols, batch_size), start=1):
        batch_rows = get_dim_company_batch(batch_symbols, symbol_to_profile, stockid_dict)
        company_rows.extend(batch_rows)
        print(f"Đã xử lý batch {batch_num} gồm {len(batch_symbols)} mã, tổng dữ liệu hiện tại: {len(company_rows)}")
        time.sleep(8)

    # cập nhật lại company_id (số thứ tự)
    for i, row in enumerate(company_rows):
        row['company_id'] = i + 1

    dim_company = pd.DataFrame(company_rows)
    # Giữ kiểu số chuẩn để lưu vào DB
    dim_company['charter_capital'] = pd.to_numeric(dim_company['charter_capital'], errors='coerce')

    engine = create_engine(conn_str)
    dim_company.to_sql(table_name, engine, if_exists='replace', index=False)
    print(f"Đã lưu dữ liệu vào bảng '{table_name}'.")
    return dim_company

with DAG(
    'vnstock_dim_company_weekly',
    default_args=default_args,
    description='Weekly update dim_company data from vnstock to DB',
    schedule_interval='@weekly',
    start_date=datetime(2025, 11, 6),
    catchup=False,
    max_active_runs=1
) as dag:
    run_dim_company_update = PythonOperator(
        task_id='get_dim_company_data',
        python_callable=get_dim_company_from_stock,
        op_kwargs={
            'dim_stock_path': '/opt/airflow/data/dim_stock.csv',
            'batch_size': 50
        }
    )
