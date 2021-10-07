"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import psycopg2
import os
from sqlalchemy import create_engine


default_args = {
    "owner": "Marcos",
    "depends_on_past": False,
    "start_date": datetime(2021, 9, 1),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
        "tutorial",
        default_args=default_args,
        schedule_interval=None
    )

TICKERS = [
    {
        'symbol': 'TSLA',
        'name': 'Tesla'
    },
    {
        'symbol': 'DIS',
        'name': 'Disney'
    },
    # {
    #     'symbol': 'XP',
    #     'name': 'XP'
    # }
]

def get_ticker_value(**kwargs):
    print(kwargs['symbol'])
    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/market/v2/get-quotes"

    querystring = {"region":"US","symbols":kwargs['symbol']}

    headers = {
        'x-rapidapi-host': "apidojo-yahoo-finance-v1.p.rapidapi.com",
        'x-rapidapi-key': "5f6d8bca03msh0af0147edb4e25fp17865fjsneddfc6257ab1"
        }

    response = requests.request("GET", url, headers=headers, params=querystring)

    to_python = json.loads(response.text)

    task_instance = kwargs['task_instance']

    task_instance.xcom_push('response', to_python)

def save_on_file(**kwargs):
    ticker = kwargs['symbol']
    data = kwargs['task_instance'].xcom_pull(task_ids='get_ticker_value_' + ticker, key='response')
    
    print(data['quoteResponse']['result'])
    file_path = '/usr/local/airflow/data/{}/{}.json'.format(ticker, datetime.now().strftime('%Y-%m-%d'))

    print(file_path)

    with open(file_path, 'w') as f:
        json.dump(data['quoteResponse']['result'], f, ensure_ascii=False)

def run_etl(**kwargs):
    print("Rodando ETL")
    file_path = os.path.abspath(os.getcwd()) + '/data/TSLA/2021-10-07.json'

    df = pd.read_json(file_path)

    df2 = df[["symbol", "priceToSales"]]

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    df2.to_sql('tickers', engine, if_exists='append')


task = {}
save_file = {}

run_etls = PythonOperator(
    task_id='run_etls',
    python_callable=run_etl,
    provide_context=True,
    dag=dag
)

for ticker in TICKERS:
    symbol = ticker['symbol']

    task[symbol] = PythonOperator(
        task_id='get_ticker_value_' + symbol,
        python_callable=get_ticker_value,
        op_kwargs={'symbol': symbol},
        provide_context=True,
        dag=dag
    )

    save_file[symbol] = PythonOperator(
        task_id='save_on_file_' + symbol,
        python_callable=save_on_file,
        op_kwargs={'symbol': symbol},
        provide_context=True,
        dag=dag
    )

    task[symbol] >> save_file[symbol]
    save_file[symbol] >> run_etls
