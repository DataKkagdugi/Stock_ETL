from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import logging
import io
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# **Step 1: 데이터 수집 및 S3 업로드**
def fetch_and_upload_minute_data():
    """API 호출 및 데이터 수집 후 S3 업로드"""
    access_token = Variable.get("korea_investment_access_token")
    exc_code = "NAS"
    sym_code = "TSLA"
    nmin = "1"
    period = 3

    all_data = pd.DataFrame()
    keyb = ""
    next_value = ""

    for _ in range(period):
        PATH = "/uapi/overseas-price/v1/quotations/inquire-time-itemchartprice"
        URL = f"https://openapi.koreainvestment.com:9443{PATH}"
        headers = {
            "content-type": "application/json",
            "authorization": f"Bearer {access_token}",
            "appkey": Variable.get("korea_investment_appkey"),
            "appsecret": Variable.get("korea_investment_appsecret"),
            "tr_id": "HHDFS76950200",
            "custtype": "P",
        }
        params = {
            "AUTH": "",
            "EXCD": exc_code,
            "SYMB": sym_code,
            "NMIN": nmin,
            "PINC": "1",
            "NEXT": next_value,
            "NREC": "120",
            "FILL": "",
            "KEYB": keyb,
        }

        response = requests.get(URL, headers=headers, params=params)
        if response.status_code != 200:
            logging.error(f"API 호출 실패: {response.status_code}")
            break
        data = response.json()
        if "output2" in data and data["output2"]:
            df = pd.DataFrame(data["output2"])
            df["datetime"] = pd.to_datetime(df["tymd"] + df["xhms"], format="%Y%m%d%H%M%S")
            df = df[["datetime", "open", "high", "low", "last", "evol", "eamt"]]
            all_data = pd.concat([all_data, df], ignore_index=True)

        next_value = data["output1"].get("next", "")
        keyb = data["output1"].get("keyb", "")

        if not next_value:
            break

    if not all_data.empty:
        s3_key = "raw_data/us_stocks_minute_data.csv"
        s3_bucket = Variable.get("s3_bucket_name")
        s3 = S3Hook(aws_conn_id="aws_conn_id")
        with io.StringIO() as csv_buffer:
            all_data.to_csv(csv_buffer, index=False)
            s3.load_string(
                string_data=csv_buffer.getvalue(),
                key=s3_key,
                bucket_name=s3_bucket,
                replace=True,
            )
        logging.info(f"데이터 S3 업로드 완료: {s3_key}")
        return s3_key
    else:
        logging.info("수집된 데이터가 없습니다.")
        return None


# **DAG 정의**
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="US_Stock_Minute_RSI_Analytics",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 11, 11),
    catchup=False,
) as dag:

    # **Task 1: 데이터 수집 및 S3 업로드**
    fetch_data_task = PythonOperator(
        task_id="fetch_and_upload_minute_data",
        python_callable=fetch_and_upload_minute_data,
    )

    # **Task 2: raw_data 테이블 생성**
    create_raw_data_table = PostgresOperator(
        task_id="create_raw_data_table",
        postgres_conn_id="redshift_dev_db",
        sql="""
        CREATE TABLE IF NOT EXISTS raw_data.us_stocks_minute (
            "datetime" TIMESTAMP,
            "open" NUMERIC(10, 4),
            "high" NUMERIC(10, 4),
            "low" NUMERIC(10, 4),
            "last" NUMERIC(10, 4),
            "evol" INT,
            "eamt" INT
        );
        """,
    )

    # **Task 3: S3 데이터를 Redshift에 적재**
    load_to_redshift = S3ToRedshiftOperator(
        task_id="load_to_raw_data",
        schema="raw_data",
        table="us_stocks_minute",
        s3_bucket=Variable.get("s3_bucket_name"),
        s3_key="raw_data/us_stocks_minute_data.csv",
        copy_options=["CSV", "IGNOREHEADER 1"],
        aws_conn_id="aws_conn_id",
        redshift_conn_id="redshift_dev_db",
    )

    # **Task 4: analytics 테이블 생성**
    create_analytics_table = PostgresOperator(
        task_id="create_analytics_table",
        postgres_conn_id="redshift_dev_db",
        sql="""
        CREATE TABLE IF NOT EXISTS analytics.us_stocks_rsi (
            "symbol" VARCHAR(10),
            "datetime" TIMESTAMP,
            "close_price" NUMERIC(10, 4),
            "volume" INT,
            "rsi" NUMERIC(10, 2),
            "status" VARCHAR(20)
        );
        """,
    )
    
    # **Task 5: analytics 변환**
    transform_to_analytics = PostgresOperator(
        task_id="transform_to_analytics",
        postgres_conn_id="redshift_dev_db",
        sql="""
        INSERT INTO analytics.us_stocks_rsi (symbol, datetime, close_price, volume, rsi, status)
        WITH base_data AS (
            SELECT 
                CAST('TSLA 'AS VARCHAR(10)) AS symbol,
                "datetime",
                "last" AS close_price,
                "evol" AS volume,
                CASE 
                    WHEN "last" > LAG("last") OVER (ORDER BY "datetime") 
                    THEN "last" - LAG("last") OVER (ORDER BY "datetime") 
                    ELSE 0 
                END AS gain_step,
                CASE 
                    WHEN "last" < LAG("last") OVER (ORDER BY "datetime") 
                    THEN LAG("last") OVER (ORDER BY "datetime") - "last" 
                    ELSE 0 
                END AS loss_step
            FROM raw_data.us_stocks_minute
        ),
        calc_rsi AS (
            SELECT
                symbol,
                "datetime",
                close_price,
                volume,
                SUM(gain_step) OVER (ORDER BY "datetime" ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS gain,
                SUM(loss_step) OVER (ORDER BY "datetime" ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) AS loss
            FROM base_data
        )
        SELECT
            symbol,
            "datetime",
            close_price,
            volume,
            100 - (100 / (1 + NULLIF(gain / NULLIF(loss, 0), 0))) AS rsi,
            CASE
                WHEN (100 - (100 / (1 + NULLIF(gain / NULLIF(loss, 0), 0)))) >= 70 THEN 'Overbought'
                WHEN (100 - (100 / (1 + NULLIF(gain / NULLIF(loss, 0), 0)))) <= 30 THEN 'Oversold'
                ELSE 'Neutral'
            END AS status
        FROM calc_rsi;
        """,
    )

    # **Task Dependency**
    fetch_data_task >> create_raw_data_table >> load_to_redshift >> create_analytics_table >> transform_to_analytics

