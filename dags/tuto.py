"""
Code that goes along with the Airflow located at:
http://airflow.readthedocs.org/en/latest/tutorial.html
"""
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json


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
    }
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

    print(to_python['quoteResponse']['result'][0]['postMarketPrice'])

task = {}

for ticker in TICKERS:
    symbol = ticker['symbol']

    task[symbol] = PythonOperator(
        task_id='get_ticker_value_' + symbol,
        python_callable=get_ticker_value,
        op_kwargs={'symbol': symbol},
        dag=dag
    )

    task[symbol]