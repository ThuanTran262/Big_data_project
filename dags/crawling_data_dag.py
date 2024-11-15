from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from datetime import timedelta

# import os
# os.environ['PYTHONPATH'] = "/home/thuantt/airflow/Big_data_project"
import sys
sys.path.insert(0, "/home/thuantt/airflow/Big_data_project")

from pipeline.crawling_data import insert_data_in_database_everyday 

dag = DAG(dag_id='etl_data',
         start_date=datetime(2024,11,10),
         schedule_interval='@daily',
         catchup=False)

# def helloWorld():
#     print('Hello World')

# dag = DAG(dag_id='hello_world_dag',
#          start_date=datetime(2024,11,10),
#          schedule_interval='@daily',
#          catchup=False)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag)
# task1 = PythonOperator(
#         task_id='hello_world',
#         python_callable=helloWorld,
#         dag=dag)

etl_data = PythonOperator(
        task_id='etl_gold_data',
        python_callable=insert_data_in_database_everyday,
        dag=dag)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# Set task dependencies
start_task >> etl_data >> end_task