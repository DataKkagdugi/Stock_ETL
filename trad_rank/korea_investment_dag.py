from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd

# 7일간의 거래량을 합산하는 함수
def get_top_stocks_by_volume(**kwargs):
    symbols = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NFLX', 'NVDA', 'INTC', 'BABA',
    'AMD', 'FB', 'TWTR', 'IBM', 'WMT', 'DIS', 'GOOG', 'BA', 'V', 'PYPL', 'NVDA', 'MSFT',
    'GOOGL', 'AMZN', 'TSLA', 'META', 'AAPL', 'NKE', 'SPY', 'AMD', 'PYPL', 'NVDA', 'WMT',
    'GOOG', 'FB', 'DIS', 'MA', 'COST', 'GE', 'UNH', 'INTC', 'BABA', 'BA', 'GS', 'JNJ',
    'HD', 'VZ', 'T', 'MCD', 'PFE', 'KO', 'WFC', 'LOW', 'XOM', 'CVX', 'TMO', 'NFLX',
    'AXP', 'AMT', 'CHTR', 'SQ', 'GS', 'LMT', 'ZTS', 'BIIB', 'PYPL', 'CAT', 'MMM', 'CSCO',
    'KO', 'PEP', 'SPGI', 'MO', 'MDT', 'AMGN', 'ISRG', 'BMY', 'V', 'RTX', 'BLK', 'TJX',
    'CVS', 'QCOM', 'MS', 'AMT', 'CME', 'COP', 'NXPI', 'AVGO', 'AMAT', 'STZ', 'SBUX',
    'UNP', 'BAX', 'CSX', 'MCK', 'SYY', 'WBA', 'UPS', 'DE', 'LULU', 'ADBE', 'MSCI',
    'PFE', 'GE', 'F', 'LRCX', 'EL', 'JPM', 'VLO', 'AON', 'BA', 'HCA', 'FIS', 'FISV',
    'SPGI', 'EXC', 'TROW', 'LHX', 'KMB', 'ADP', 'ICE', 'NKE', 'MU', 'NEM', 'TSN',
    'CHTR', 'TDOC', 'AMGN', 'TMO', 'KHC', 'FSLR', 'MO', 'BKNG', 'GM', 'AZO', 'INTU',
    'FIS', 'FLIR', 'CVS', 'CL', 'CLX', 'ADSK', 'VRTX', 'VRTX', 'MAR', 'CTSH', 'RMD',
    'PLD', 'WDAY', 'WELL', 'OXY', 'WFC', 'HUM', 'FTNT', 'UAL', 'TGT', 'SIVB', 'VEEV',
    'BIIB', 'TRV', 'CMG', 'ABBV', 'KMI', 'PGR', 'ETSY', 'BURL', 'WDC', 'HCA', 'NFLX',
    'NOK', 'WMT', 'MSFT', 'GS', 'GOOGL', 'AAPL', 'BRK.B', 'V', 'HD', 'INTC', 'AMD',
    'AMZN', 'TXN', 'SPY', 'SBUX', 'PYPL', 'DIS', 'VZ', 'TSLA', 'BA', 'NVDA', 'PFE',
    'CVX', 'NEE', 'AMT', 'RTX', 'MA', 'EXC', 'MDT', 'CSCO', 'QCOM', 'UNH', 'COP', 'PEP',
    'JNJ', 'BMY', 'LMT', 'WBA', 'MO', 'F', 'XOM', 'CVS', 'UPS', 'MCD', 'UNP', 'COST',
    'WFC', 'LOW', 'DE', 'MCK', 'NKE', 'BAX', 'SCHW', 'QCOM', 'ISRG', 'TMO', 'GE', 'SYY',
    'MKC', 'TROW', 'BUD', 'V', 'HCA', 'AMD', 'GOOG', 'MS', 'SPY', 'V', 'ZTS', 'LULU', 'UNP',
    'AMZN', 'BABA', 'DIS', 'GM', 'BIDU', 'SNE', 'PFE', 'FISV', 'PG', 'HUM', 'KO', 'TGT',
    'CVS', 'RMD', 'AMGN', 'BIIB', 'JNJ', 'PYPL', 'LMT', 'CTSH', 'BIIB', 'VZ', 'RHHBY',
    'TGT', 'STZ', 'AMT', 'MU', 'LRCX', 'TT', 'ROST', 'WMT', 'ZBH', 'FIS', 'WFC', 'NEM',
    'LPL', 'TSLA', 'GOOG', 'QCOM', 'JPM', 'PEP', 'DIS', 'ADBE', 'V', 'DDOG', 'SMH', 'XOM',
    'KHC', 'FIS', 'ORCL', 'AAPL', 'VLO', 'KMB', 'AMZN', 'FIS', 'MCD', 'PG', 'WBA', 'FTNT',
    'CME', 'TSN', 'WMT', 'EXC', 'SBUX', 'AON', 'MMM', 'MO', 'LOW', 'MA', 'HCA', 'AVGO',
    'WBA', 'PYPL', 'GOOG', 'TMO', 'RTX', 'AXP', 'COST'
] # 조회할 주식 종목
    results = []
    for symbol in symbols:
        end_date = datetime.now()
        start_date = end_date - timedelta(days=7)
        data = yf.download(symbol, start=start_date, end=end_date)
        total_volume = data["Volume"].sum()
        results.append((symbol, total_volume))
    
    results = sorted(results, key=lambda x: x[1], reverse=True)
    print("Top Stocks by Volume:", results)
    return results

# DAG 정의
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'korea_investment_dag',
    default_args=default_args,
    description='A DAG to get top stocks by volume',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id='get_top_stocks_task',
        python_callable=get_top_stocks_by_volume,
        provide_context=True,
    )
