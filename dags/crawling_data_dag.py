from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pipeline.

def helloWorld():
    print('HelloWorld')
    

with DAG(dag_id='hello_world_dag',
         start_date=datetime(2024,11,10),
         schedule='@hourly',
         catchup=False) as dag:
    task1 = PythonOperator(
        task_id='hello_word',
        python_callable=helloWorld
    )

task1