from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
from airflow.utils.dates import days_ago
import logging

logger = logging.getLogger('airflow.task')

def get_s3_connection(autocommit=False):
    s3_hook = S3Hook('aws_connection')
    return s3_hook.get_conn()

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_connection')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

# DAG 정의
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 27),
    'retries': 1,
}

dag = DAG(
    'project3_stockchange_dag',
    default_args=default_args,
    description='Load data from S3 to Redshift and create an analytics table',
    schedule_interval=None,  # 수동 실행
    catchup=False,
    start_date=days_ago(1),
)

# 1. 테이블 생성 작업
create_table_task = PostgresOperator(
        task_id='create_table_task',
        postgres_conn_id='redshift_connection',
        dag=dag,
        sql="""
            CREATE TABLE IF NOT EXISTS raw_data.stock_change (
                stock_symbol VARCHAR(10) NOT NULL,
                price_change_rate FLOAT,
                load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

# 2. S3에서 Redshift raw_data 테이블로 데이터 로드 (COPY 명령어 사용)
load_raw_data = S3ToRedshiftOperator(
    task_id='load_raw_data',
    schema='raw_data',
    table='stock_change',
    s3_bucket='project3-team8-bucket-bdj',
    s3_key='us_stock_change_rate.csv',
    copy_options=['CSV', 'IGNOREHEADER 1'],
    redshift_conn_id='redshift_connection',
    aws_conn_id='aws_connection',
    method='REPLACE',
    dag=dag,
)

# 3. 급증/급감한 상위 10개 종목을 analytics 테이블에 저장 (CTAS 사용)
def create_analytics_table(**kwargs):
    cursor = get_Redshift_connection(autocommit=True)
    try:
        # CTAS를 사용하여 급증 및 급감 테이블 생성
        cursor.execute("""
            CREATE OR REPLACE TABLE analytics.top_stock_change AS
            SELECT stock_symbol, price_change_rate
            FROM raw_data.stock_change
            WHERE price_change_rate IS NOT NULL
            ORDER BY price_change_rate DESC
            LIMIT 10
            UNION ALL
            SELECT stock_symbol, price_change_rate
            FROM raw_data.stock_change
            WHERE price_change_rate IS NOT NULL
            ORDER BY price_change_rate ASC
            LIMIT 10;
        """)
        logger.info("Analytics table for top 10 rising and top 10 falling stocks created successfully.")
    except Exception as e:
        logger.error(f"Error creating analytics table: {e}")
    finally:
        cursor.close()

create_analytics = PythonOperator(
    task_id='create_analytics_table',
    python_callable=create_analytics_table,
    provide_context=True,
    dag=dag,
)

# 작업 순서 설정
create_table_task >> load_raw_data >> create_analytics
