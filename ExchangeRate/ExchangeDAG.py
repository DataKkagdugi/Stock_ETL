from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import boto3, csv, os, requests
from pathlib import Path

#로컬 파일 경로
LOCAL_FILE_PATH = Path(Variable.get("local_file_path", default_var="/exchange_rate.csv"))


def get_s3_connection(autocommit=False):
    s3_hook = S3Hook('project_s3_conn')
    return s3_hook.get_conn()

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db_project')
    conn = hook.get_conn()
    conn.autocommit=autocommit
    return conn.cursor()

# AWS S3 설정
S3_BUCKET = r"project3-team8-bucket-bdj"
S3_KEY = r"currency/exchange_rate.csv"


# API 설정
API_URL_TEMPLATE = "https://www.koreaexim.go.kr/site/program/financial/exchangeJSON?authkey={api_key}&searchdate={date}&data=AP01"

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id="exchange_rate_etl",
    default_args=default_args,
    schedule_interval="0 9 * * *",  # 매일 오전 9시 실행
    catchup=False,  # 과거 실행 생략
    tags=["ETL", "API"],
) as dag:

    @task
    def extract_transform(api_key, execution_date, next_execution_date):
        """
        1. API 데이터를 추출하고 변환
        2. 로컬 CSV 파일로 저장
        """
        start_date = execution_date
        end_date = next_execution_date - timedelta(days=1)
        current_date = start_date
        records = []

        while current_date <= end_date:
            # API 호출
            formatted_date = current_date.strftime("%Y%m%d")
            response = requests.get(API_URL_TEMPLATE.format(api_key=api_key, date=formatted_date), verify=False)
            if response.status_code != 200:
                raise Exception(f"API 호출 실패: {response.status_code}")
            
            data = response.json()
            for item in data:
                if item['cur_unit'] == "USD":  # USD만 필터링
                    records.append([
                        item['cur_unit'],  # currency_code
                        "KRW",  # base_currency
                        float(item['deal_bas_r'].replace(",", "")),  # exchange_rate
                        current_date.strftime("%Y-%m-%d"),  # reference_date
                    ])
            current_date += timedelta(days=1)

        # 로컬 파일 저장
        with open(LOCAL_FILE_PATH, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["currency_code", "base_currency", "exchange_rate", "reference_date"])
            writer.writerows(records)

        return LOCAL_FILE_PATH

    @task
    def upload_to_s3(file_path):
        """
        3. 변환된 데이터를 S3에 업로드
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        s3 = boto3.client('s3')
        s3.upload_file(file_path, S3_BUCKET, S3_KEY)
        return f"s3://{S3_BUCKET}/{S3_KEY}"

    # S3 -> Redshift
    load_to_redshift = S3ToRedshiftOperator(
        task_id="load_to_redshift",
        schema="raw_data",
        table="currency",
        s3_bucket=S3_BUCKET,
        s3_key=S3_KEY,
        copy_options=["csv", "IGNOREHEADER 1"],
        aws_conn_id="aws_default",
        redshift_conn_id="redshift_default"
    )

    # 데이터 변환
    transform_to_analytics = PostgresOperator(
        task_id="transform_to_analytics",
        postgres_conn_id="redshift_default",
        sql="""
        INSERT INTO analytics.currency_analytics (currency_code, base_currency, avg_exchange_rate, reference_month)
        SELECT
            currency_code,
            base_currency,
            AVG(exchange_rate) AS avg_exchange_rate,
            DATE_TRUNC('month', reference_date) AS reference_month
        FROM raw_data.currency
        GROUP BY currency_code, base_currency, DATE_TRUNC('month', reference_date);
        """
    )

    # Task 간 의존성 설정
    API_KEY = Variable.get("exchange_rate_api_key")
    records_file = extract_transform(API_KEY, '{{ execution_date }}', '{{ next_execution_date }}')
    s3_path = upload_to_s3(records_file)
    records_file >> s3_path >> load_to_redshift >> transform_to_analytics