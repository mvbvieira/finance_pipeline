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
        "bra_tickers_pipeline",
        default_args=default_args,
        schedule_interval=None
    )

TICKERS = [
    {
        'symbol': 'CPLE6.SA',
        'name': 'COPEL',
        'symbol_B3': 'CPLE6'
    },
    {
        'symbol': 'DEVA11.SA',
        'name': 'Devant Recebiveis Imobiliarios Fundo De Investimento Imobiliario',
        'symbol_B3': 'DEVA11'
    }
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

def save_on_file(**kwargs):
    ticker = kwargs['symbol']
    symbol_b3 = kwargs['symbol_b3']
    data = kwargs['task_instance'].xcom_pull(task_ids='get_ticker_value_' + ticker, key='response')
    current_date = kwargs['task_instance'].xcom_pull(task_ids='get_current_date', key='date')
    
    print(data['quoteResponse']['result'])
    file_path = '/usr/local/airflow/data/BRA/{}/{}.json'.format(symbol_b3, current_date)

    print(file_path)

    with open(file_path, 'w') as f:
        json.dump(data['quoteResponse']['result'], f, ensure_ascii=False)

def run_etl(**kwargs):
    ticker = kwargs['symbol']
    symbol_b3 = kwargs['symbol_b3']
    current_date = kwargs['task_instance'].xcom_pull(task_ids='get_current_date', key='date')

    file_path = os.path.abspath(os.getcwd()) + '/data/BRA/{}/{}.json'.format(symbol_b3, current_date)

    df = pd.read_json(file_path)

    df2 = df[["symbol", "bid"]]
    df2['ExecutionDate'] = current_date

    engine = create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    df2.to_sql('bra_tickers', engine, if_exists='append')


task = {}
save_file = {}
run_etls = {}

get_current_date = PythonOperator(
    task_id='get_current_date',
    python_callable=save_current_date,
    provide_context=True,
    dag=dag
)


for ticker in TICKERS:
    symbol = ticker['symbol']
    symbol_B3 = ticker['symbol_B3']

    task[symbol] = PythonOperator(
        task_id='get_ticker_value_' + symbol,
        python_callable=get_ticker_value,
        op_kwargs={'symbol': symbol, 'symbol_b3': symbol_B3},
        provide_context=True,
        dag=dag
    )

    save_file[symbol] = PythonOperator(
        task_id='save_on_file_' + symbol,
        python_callable=save_on_file,
        op_kwargs={'symbol': symbol, 'symbol_b3': symbol_B3},
        provide_context=True,
        dag=dag
    )

    run_etls[symbol] = PythonOperator(
        task_id='run_etls_' + symbol,
        python_callable=run_etl,
        op_kwargs={'symbol': symbol, 'symbol_b3': symbol_B3},
        provide_context=True,
        dag=dag
    )

    get_current_date >> task[symbol]
    task[symbol] >> save_file[symbol]
    save_file[symbol] >> run_etls[symbol]
