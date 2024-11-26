from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

chrome_options = Options()
chrome_options.add_experimental_option("detach", True)

from datetime import datetime
import psycopg2

def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='project_redshift_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()

def Get_today_fgi(): 
    #셀레니움으로 페이지 연결
    BASE_URL = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"

    fgi_data = []
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(BASE_URL)
    # 페이지 소스를 가져옵니다
    data= driver.page_source[118:191]
    fgi = data[11:25]
    rating = data[-34:-20]

    #transform rating and types
    index = rating.index("\"")
    rating = rating[:index]
    print(rating)

    #add date info
    now = datetime.now().date()
    print(now)

    fgi_data.append([now, fgi, rating])
    return fgi_data

def Transform_and_Load():
    # analytics 스키마 적재 Task
    cur = get_Redshift_connection()
    try:
        cur.execute("BEGIN;")
        date, index, rating = fgi_data[0]
        print(date, index, rating)
        sql = f"INSERT INTO {schema}.{table} VALUES ('{date}', '{index}', '{rating}')"
        cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")   

dag = DAG(
    dag_id = 'project3_fgi',
    start_date = datetime(2023,11,22),
    catchup=False,
    schedule = '0 8 * * *')

Get_today_fgi = PythonOperator(
    task_id = 'Get_today_fgi',
    #python_callable param points to the function you want to run 
    python_callable = Get_today_fgi,
    #dag param points to the DAG that this task is a part of
    dag = dag)

Transform_and_Load = PythonOperator(
    task_id = 'Transform_and_Load',
    python_callable = Transform_and_Load,
    params = {
        'schema': 'raw_data',
        'table': 'fear_and_greed_index'
    },
    dag = dag)

#Assign the order of the tasks in our DAG
Get_today_fgi >> Transform_and_Load