import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

# 조회할 미국 주식 티커 리스트 (200개로 확장)
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
]

# 7일간의 거래량을 합산하여 반환하는 함수
def get_7_day_volume(symbol):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=7)
    
    # 주식 데이터 조회 (7일 간의 데이터)
    stock_data = yf.download(symbol, start=start_date, end=end_date)
    
    # 7일간 거래량 합산 (정수형으로 변환)
    total_volume = int(stock_data['Volume'].sum())  # 거래량 컬럼의 합산
    return total_volume

# 7일간 거래량이 많은 순서대로 정렬
def get_top_stocks_by_volume(symbols):
    stock_volumes = {}
    
    # 각 종목에 대해 7일간의 거래량 합산
    for symbol in symbols:
        volume = get_7_day_volume(symbol)
        if symbol not in stock_volumes or volume > stock_volumes[symbol]:  # 중복 체크 및 높은 거래량만 저장
            stock_volumes[symbol] = volume
    
    # 거래량 순으로 정렬
    sorted_stocks = sorted(stock_volumes.items(), key=lambda x: x[1], reverse=True)
    
    return sorted_stocks

# 7일 거래량 순위 출력 (순위 인덱스 추가)
top_stocks = get_top_stocks_by_volume(symbols)
print("7일 거래량 순위:")
for index, (stock, volume) in enumerate(top_stocks, start=1):
    print(f"Rank {index}: {stock} - {volume} 거래량")
