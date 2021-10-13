"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import psycopg2
import os
from sqlalchemy import create_engine
import os.path
from os import path


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


def save_current_date(**kwargs):
    current_date = datetime.now().strftime('%Y-%m-%d')

    task_instance = kwargs['task_instance']

    task_instance.xcom_push('date', current_date)

def get_ticker_value(**kwargs):
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

def verify_folder(**kwargs):
    task_name = kwargs['task_instance'].task_id.replace("folder_exists_", "")
    ticker = task_name
    path_name = ticker

    if(path.exists('data/{}'.format(path_name))):
        return "created_folder_" + ticker
    else:
        return "need_create_folder_" + ticker

def get_create_folder(**kwargs):
# Directory
    directory = kwargs['symbol']
# Parent Directory path
    parent_dir = "data/"
# Path
    path = os.path.join(parent_dir, directory)

    os.mkdir(path)


def save_on_file(**kwargs):
    ticker = kwargs['symbol']
    data = kwargs['task_instance'].xcom_pull(task_ids='get_ticker_value_' + ticker, key='response')
    current_date = kwargs['task_instance'].xcom_pull(task_ids='get_current_date', key='date')
    
    print(data['quoteResponse']['result'])
    file_path = '/usr/local/airflow/data/{}/{}.json'.format(ticker, current_date)

    print(file_path)

    with open(file_path, 'w') as f:
        json.dump(data['quoteResponse']['result'], f, ensure_ascii=False)

def run_etl(**kwargs):
    ticker = kwargs['symbol']
    current_date = kwargs['task_instance'].xcom_pull(task_ids='get_current_date', key='date')

    file_path = os.path.abspath(os.getcwd()) + '/data/{}/{}.json'.format(ticker, current_date)

    df = pd.read_json(file_path)

    df2 = df[["symbol", "priceToSales"]]
    df2['ExecutionDate'] = current_date

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    df2.to_sql('tickers', engine, if_exists='append')


task = {}
save_file = {}
run_etls = {}
folder_exists = {}

get_current_date = PythonOperator(
    task_id='get_current_date',
    python_callable=save_current_date,
    provide_context=True,
    dag=dag
)


for ticker in TICKERS:
    symbol = ticker['symbol']
    need_create_folder = {}
    created_folder = {}
    create_folder = {}

    need_create_folder[symbol] = DummyOperator(
        task_id="need_create_folder_" + symbol,
        trigger_rule='none_failed',
        dag=dag
    )

    created_folder[symbol] = DummyOperator(
        task_id="created_folder_" + symbol,
        trigger_rule='none_failed',
        dag=dag
    )

    task[symbol] = PythonOperator(
        task_id='get_ticker_value_' + symbol,
        python_callable=get_ticker_value,
        op_kwargs={'symbol': symbol},
        provide_context=True,
        dag=dag
    )

    folder_exists[symbol] = BranchPythonOperator(
        task_id='folder_exists_' + symbol,
        provide_context=True,
        python_callable=verify_folder,
        dag=dag
    )


    create_folder[symbol] = PythonOperator(
        task_id='create_folder_' + symbol,
        python_callable=get_create_folder,
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

    run_etls[symbol] = PythonOperator(
        task_id='run_etls_' + symbol,
        python_callable=run_etl,
        op_kwargs={'symbol': symbol},
        provide_context=True,
        dag=dag
    )

    get_current_date >> task[symbol]
    task[symbol] >> folder_exists[symbol]
    folder_exists[symbol] >> need_create_folder[symbol] >> create_folder[symbol] >> created_folder[symbol] >> save_file[symbol]
    folder_exists[symbol] >> created_folder[symbol] >> save_file[symbol]
    save_file[symbol] >> run_etls[symbol]
