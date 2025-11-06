from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
from vnstock import Company
import pandas as pd
import warnings

warnings.simplefilter(action='ignore', category=FutureWarning)

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

def parse_float(val):
    if val is None or pd.isna(val):
        return None
    try:
        val_str = str(val).replace(',', '').strip()
        return float(val_str)
    except Exception:
        return None

def get_batches(lst, batch_size):
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]

def get_dim_share_structure_batch(batch_symbols, stockid_dict):
    share_rows = []
    for symbol in batch_symbols:
        stock_id = stockid_dict.get(symbol, None)
        # Lấy issue_share từ TCBS
        try:
            tcb_company = Company(symbol=symbol, source='TCBS').overview()
            issue_share = parse_float(get_value_safe(tcb_company, 'issueShare'))
        except Exception:
            issue_share = None

        # Lấy charter_capital và financial_ratio_issue_share từ VCI
        try:
            time.sleep(2)
            vci_company = Company(symbol=symbol, source='VCI').overview()
            charter_capital = parse_float(get_value_safe(vci_company, 'charter_capital'))
            vci_issue_share = parse_float(get_value_safe(vci_company, 'issue_share'))
            if issue_share is None and vci_issue_share is not None:
                issue_share = vci_issue_share
            financial_ratio_issue_share = parse_float(get_value_safe(vci_company, 'financial_ratio_issue_share'))
        except Exception:
            charter_capital = None
            financial_ratio_issue_share = None

        share_rows.append({
            'share_id': None,  # sẽ cập nhật lại sau theo thứ tự batch
            'stock_id': stock_id,
            'issue_share': issue_share,
            'charter_capital': charter_capital,
            'financial_ratio_issue_share': financial_ratio_issue_share,
        })
    return share_rows

def get_dim_share_structure_from_stock(
    dim_stock_path='/opt/airflow/data/dim_stock.csv',
    save_path='/opt/airflow/data/dim_share_structure.csv',
    batch_size=50, **kwargs
):
    df_stock = pd.read_csv(dim_stock_path, encoding='utf-8-sig')
    symbols = df_stock['symbol'].tolist()
    stockid_dict = dict(zip(df_stock['symbol'], df_stock['stock_id']))
    all_rows = []

    for batch_num, batch_symbols in enumerate(get_batches(symbols, batch_size), start=1):
        batch_rows = get_dim_share_structure_batch(batch_symbols, stockid_dict)
        all_rows.extend(batch_rows)
        print(f"Đã xử lý batch {batch_num} gồm {len(batch_symbols)} mã, tổng: {len(all_rows)}")
        time.sleep(8)  # nghỉ giữa các batch

    for i, row in enumerate(all_rows):
        row['share_id'] = i + 1

    dim_share_structure = pd.DataFrame(all_rows)
    # Định dạng số đẹp hơn khi lưu file
    dim_share_structure['issue_share'] = dim_share_structure['issue_share'].apply(lambda x: '{:,.0f}'.format(x) if pd.notna(x) and x is not None else '')
    dim_share_structure['charter_capital'] = dim_share_structure['charter_capital'].apply(lambda x: '{:,.0f}'.format(x) if pd.notna(x) and x is not None else '')
    dim_share_structure['financial_ratio_issue_share'] = dim_share_structure['financial_ratio_issue_share'].apply(lambda x: '{:.4f}'.format(x) if pd.notna(x) and x is not None else '')

    dim_share_structure.to_csv(save_path, index=False, encoding='utf-8-sig', lineterminator='\n', quoting=1)
    print(f"Đã lưu file CSV: {save_path}")
    return dim_share_structure

with DAG(
    'vnstock_dim_share_structure_weekly',
    default_args=default_args,
    description='Weekly update dim_share_structure from vnstock',
    schedule_interval='@weekly',
    start_date=datetime(2025, 11, 6),
    catchup=False,
    max_active_runs=1
) as dag:
    run_dim_share_structure_update = PythonOperator(
        task_id='get_dim_share_structure',
        python_callable=get_dim_share_structure_from_stock,
        op_kwargs={
            'dim_stock_path': '/opt/airflow/data/dim_stock.csv',
            'save_path': '/opt/airflow/data/dim_share_structure.csv',
            'batch_size': 50
        }
    )
