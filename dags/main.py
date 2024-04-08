# -*- coding: utf-8 -*-
"""
Created on Fri Mar 29 10:05:16 2024

@author: Facu
"""
import pandas as pd
import requests
from sqlalchemy import create_engine
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


username='facundo_villafane_59_coderhouse'
password='p8658bXK6I'
host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com'
database='data-engineer-database'
function = 'TIME_SERIES_DAILY'
companies=['YPF','GOOG','KO']
key='6UUVKQTY9GOWIFGO'

def import_transform_alphavantage_data(company,function, key):
    url = 'https://www.alphavantage.co/query?function='+function+'&symbol='+company+'&apikey='+key
    r = requests.get(url)
    data=r.json()
    dfc = pd.DataFrame(data["Time Series (Daily)"])
    dfc = dfc.T
    dfc['Company']=data['Meta Data']['2. Symbol']
    dfc=dfc.reset_index().rename(columns={"index":"Date","1. open":"Open Value","2. high":"Highest Value","3. low":"Lowest Value","4. close":"Close Value","5. volume":"Volume"})
    dfc = dfc.astype({'Date':'datetime64[ns]','Open Value':'float32','Highest Value':'float32','Lowest Value':'float32','Close Value':'float32','Volume':'int32'})
    return dfc


def get_data_store():
    df = pd.DataFrame()
    for company in companies:
        df=pd.concat([import_transform_alphavantage_data(company,function,key),df])
        df['Timestamp'] = pd.Timestamp("now") 
    conn = create_engine('postgresql://'+username+':'+password+'@'+host+':5439/'+database)
    df.to_sql('alpha_vantage_shares', conn, index=False, if_exists='replace')
    
    


default_args={
    'owner': 'FacuVillafañe',
    'retries': 5,
    'retry_delay': timedelta(minutes=1) 
}

api_dag = DAG(
        dag_id="TercerEntrega",
        default_args= default_args,
        description="DAG para consumir API y vaciar datos en Redshift",
        start_date=datetime(2024,3,3,3),
        schedule_interval='@daily' 
    )

task1 = BashOperator(task_id='start_task',
    bash_command='echo Start'
)

task2 = PythonOperator(
    task_id='download_data',
    python_callable=get_data_store,
    dag=api_dag,
)

task3 = BashOperator(
    task_id= 'end_tarea',
    bash_command='echo Finished'
)
task1 >> task2 >> task3


