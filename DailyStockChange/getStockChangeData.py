import urllib.request
import json
import mojito
import pprint
import pandas as pd

# 전체 미국 주식 종목 리스트 저장
def get_all_us_stock_codes():
    USA_all_stocks_list = []

    # 나스닥 거래소 API
    NASDAQ_basic_url = "https://api.stock.naver.com/stock/exchange/NASDAQ/marketValue"
    NASDAQ_raw_data = urllib.request.urlopen(NASDAQ_basic_url).read()
    NASDAQ_raw_json_data = json.loads(NASDAQ_raw_data)
    NASDAQ_totalcount = NASDAQ_raw_json_data['totalCount']
    NASDAQ_page_count = (NASDAQ_totalcount // 100) + 1
    
    def NASDAQ_Stocks_get(page):
        NASDAQ_url = f"https://api.stock.naver.com/stock/exchange/NASDAQ/marketValue?page={page}&pageSize=100"
        NASDAQ_raw_data = urllib.request.urlopen(NASDAQ_url).read()
        NASDAQ_raw_json_data = json.loads(NASDAQ_raw_data)
        for each in NASDAQ_raw_json_data['stocks']:
            symbolCode = each['symbolCode']
            USA_all_stocks_list.append(symbolCode)

    for page in range(1, NASDAQ_page_count + 1):
        NASDAQ_Stocks_get(page)

    return USA_all_stocks_list

# 전체 미국 주식 종목 코드 가져오기
all_us_stock_codes = get_all_us_stock_codes()
print(all_us_stock_codes)

key = "PS5WY4538T9PviRU7WXqyKHL13mewtCV6ySt"
secret = "gwLx3HFLvwQ6fHMW8D8iOdYhSvsAefQ002oPLLT/TEl78POxv1xmxL18SmA/uxdEsENpnCXHrIubcsjrZnBexSo9Ir49EFnKyxQW9RSg8cxZcDI0XLQ2cbA8zk5x0PoTJQ+fFjPofaBPITsuv+p1F7JsHKfETC+mfZd0nfyphcBLqVUuXIM="
acc_no = "50122110-01"

# 브로커 초기화
broker = mojito.KoreaInvestment(
    api_key=key,
    api_secret=secret,
    acc_no=acc_no,
    exchange='나스닥'
)

# 데이터 저장용 리스트
stock_data = []

# 종목별 데이터 조회
for stock in all_us_stock_codes:
    try:
        # 가격 데이터 조회
        price = broker.fetch_price(stock)

        # 필요한 데이터 추출
        output = price.get("output", {})
        if not output:
            raise ValueError("output 데이터가 없습니다.")

        current_price = float(output.get("last", 0))  # 현재가
        previous_price = float(output.get("base", 0))  # 전일 종가
        change_rate = float(output.get("rate", 0))  # 증감률

        # 데이터 저장
        stock_data.append({
            "Stock": stock,  # 종목 코드
            "Current Price": current_price,
            "Previous Price": previous_price,
            "Change Rate (%)": change_rate
        })

    except Exception as e:
        print(f"데이터 조회 실패 - 종목: {stock}, 오류: {e}")

# DataFrame 생성
df = pd.DataFrame(stock_data)

# 결과 출력
print("\n--- 종목별 전날 대비 증감량 데이터 ---\n")
print(df.head(20))

# CSV 파일로 저장
df.to_csv("us_stock_change_rate.csv", index=False, encoding="utf-8-sig")
print("\n데이터가 'us_stock_change_rate.csv'에 저장되었습니다.")
