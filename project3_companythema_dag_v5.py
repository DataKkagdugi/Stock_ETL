from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pandas import Timestamp
import io
from io import StringIO

import yfinance as yf
import pandas as pd
import logging

def get_s3_connection(autocommit=False):
    s3_hook = S3Hook('project_s3_conn')
    return s3_hook.get_conn()

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db_project')
    conn = hook.get_conn()
    conn.autocommit=autocommit
    return conn.cursor()

def read_csv_and_yf(bucket_name: str, key:str) -> pd.DataFrame:
    s3_client = get_s3_connection()
    obj = s3_client.get_object(Bucket=bucket_name, Key=key)
    body = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(body))
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=7)
    stock=yf.download(df['Symbol'].tolist(), start=start_date, end=end_date)
    df_v = stock['Volume'].reset_index()
    df_new = df_v.melt(id_vars='Date',var_name='ticker',value_name='volume')

    csv_buffer = StringIO()
    df_new.to_csv(csv_buffer, index=False)

    s3_client.put_object(
        Bucket=bucket_name,
        Key='volume_data.csv',
        Body=csv_buffer.getvalue()
    )

    return 'volume_data.csv'

def create_volume_to_redshift():
    cursor = get_Redshift_connection()
    # 테이블 생성 (이미 존재할 경우 무시)
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS raw_data.volume (
        Date TIMESTAMP,
        Symbol VARCHAR(200),
        Volume BIGINT 
    );
    """
    cursor.execute(create_table_query)

    return True

with DAG(
    dag_id='project3_companythema_dag_v5',
    schedule_interval='0 0 * * *', # 매일0시 0분
    start_date=datetime(2024,11,1),
    catchup=False,
    default_args = {
        'retries': 1
    }
) as dag:
    
    create_table_redshift = PostgresOperator(
        task_id='create_table_redshift',
        postgres_conn_id='redshift_dev_db_project',
        sql="""
        CREATE TABLE IF NOT EXISTS raw_data.nasdaq_ticker (
            Symbol VARCHAR(200),
            Name VARCHAR(200),
            Last_Sale VARCHAR(200),
            Net_Change FLOAT,
            Percent_Change FLOAT,
            Market_Cap FLOAT,
            Country VARCHAR(200),
            IPO_Year INT,
            Volume BIGINT,
            Sector VARCHAR(200),
            Industry VARCHAR(200)
        );
        """
    )

    load_data_s3_to_red = S3ToRedshiftOperator(
        task_id='load_data_s3_to_red',
        schema='raw_data',
        table='nasdaq_ticker',
        s3_bucket='project3-team8-bucket-bdj',
        s3_key='modified_nasdaq_ticker.csv',
        copy_options=['CSV','IGNOREHEADER 1'],
        redshift_conn_id='redshift_dev_db_project',
        aws_conn_id='project_s3_conn',
        method='REPLACE'
    )

    read_csv_and_yf_task = PythonOperator(
        dag=dag,
        task_id='read_csv_and_yf',
        python_callable=read_csv_and_yf,
        op_kwargs={
            'bucket_name': 'project3-team8-bucket-bdj',
            'key': 'modified_nasdaq_ticker.csv'
        },
        provide_context=True
    )

    create_volume_to_redshift_task = PythonOperator(
        dag=dag,
        task_id='create_volume',
        python_callable=create_volume_to_redshift
    )

    load_data_s3_to_red2 = S3ToRedshiftOperator(
        task_id='load_data_s3_to_red2',
        schema='raw_data',
        table='volume',
        s3_bucket='project3-team8-bucket-bdj',
        s3_key='volume_data.csv',
        copy_options=['CSV','IGNOREHEADER 1'],
        redshift_conn_id='redshift_dev_db_project',
        aws_conn_id='project_s3_conn',
        method='REPLACE'
    )

    drop_table_task = PostgresOperator(
        dag=dag,
        task_id='drop_table',
        postgres_conn_id='redshift_dev_db_project',
        sql='DROP TABLE IF EXISTS analytics.thema_volume;'
    )

    join_tables_and_create_task = PostgresOperator(
        dag=dag,
        task_id='join_tables_and_create',
        postgres_conn_id='redshift_dev_db_project',
        sql="""
        CREATE TABLE analytics.thema_volume AS
        SELECT B.date, A.sector, SUM(B.volume) as total_volume
        FROM raw_data.nasdaq_ticker A 
        JOIN raw_data.volume B ON A.symbol = B.symbol
        WHERE A.sector IS NOT NULL
        GROUP BY A.sector, B.date
        ORDER BY B.date;
        """
    )

    create_table_redshift >> load_data_s3_to_red >> read_csv_and_yf_task >> create_volume_to_redshift_task >> load_data_s3_to_red2 >> drop_table_task >> join_tables_and_create_task