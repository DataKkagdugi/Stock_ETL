from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options

import json
from datetime import datetime
import psycopg2

chrome_options = Options()
chrome_options.add_experimental_option("detach", True)
chrome_options.add_argument('--headless')
chrome_options.add_argument('--no-sandbox')
chrome_options.add_argument('--disable-dev-shm-usage')

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='project_redshift_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def Get_today_fgi(): 
    #셀레니움으로 페이지 연결
    BASE_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

    fgi_data = []
    remote_webdriver = 'remote_chromedriver'
    with webdriver.Remote(f'{remote_webdriver}:4444/wd/hub', options=chrome_options) as driver:
        driver.get(BASE_URL)
    # 페이지 소스를 가져옵니다
        string_data= driver.page_source
        json_data = json.loads(string_data)
        today_data = json_data.get("fear_and_greed")
        
        fgi = today_data["score"]
        rating = today_data["rating"]

        #rating 변환
        rating = rating.upper()
        print(rating)

        #fgi 반올림
        fgi = int(float(fgi))
        #add date info
        now = datetime.now().date()
        print(now)
        now = now.isoformat()

        fgi_data.append([now, fgi, rating])
        return fgi_data

def Transform_and_Load(**context):
    # raw_data 스키마 적재 Task

    ti = context['ti']
    fgi_data = ti.xcom_pull(task_ids='Get_today_fgi')

    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        date, index, rating = fgi_data[0]
        print(date, index, rating)
        schema = context['params']['schema']
        table = context['params']['table']
        sql = f"""
        DELETE FROM {schema}.{table} WHERE date = '{date}';
        INSERT INTO {schema}.{table} VALUES ('{date}', '{index}', '{rating}');
"""
        cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")   

dag = DAG(
    dag_id = 'project3_fgi',
    start_date = datetime(2023,11,22),
    catchup=False,
    schedule = '0 20 * * *')

Get_today_fgi = PythonOperator(
    task_id = 'Get_today_fgi',
    #python_callable param points to the function you want to run 
    python_callable = Get_today_fgi,
    provide_context=True,
    #dag param points to the DAG that this task is a part of
    dag = dag)

Transform_and_Load = PythonOperator(
    task_id = 'Transform_and_Load',
    python_callable = Transform_and_Load,
    provide_context=True,
    params = {
        'schema': 'raw_data',
        'table': 'fear_and_greed_index'
    },
    dag = dag)

#Assign the order of the tasks in our DAG
Get_today_fgi >> Transform_and_Load
