from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import requests
import json

def captura_conta_dados():
  url = 'https://data.cityofnewyork.us/resource/rc75-m7u3.json'
  response = request.get(url)
  df = pd.DataFrame(json.loads(response.content))
  qtd = len(df.index)
  return qtd

def is_valid():
  qtd = ti.xcom_pull(task_ids = 'capture_data_count')
  if (qtd > 1000):
    return 'valid'
  return 'n_valid'  

with DAG('tutotial_dag', start_date = datetime(2021,12,1),
      schedule_interval = '30 * * * *', catchup = False) as dag

  capture_data_count = PythonOperator(
    task_id = 'capture_data_count',
    python_callable = capture_data_count
  )
  is_valid = BranchOperator(
    task_id = 'is_valid'
    python_calleble = is_valid

  valid = BashOperator(
    task_id = 'valid',
    bash_command = "echo 'Quantity ok'"
  )

  nvalid = BashOperator(
    task_id = 'nvalid',
    bash_command = "echo 'Quantity not ok'"
  )

capture_data_count >> is_valid [valid, nvalid]
