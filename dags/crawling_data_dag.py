from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from datetime import timedelta


import sys
sys.path.insert(0, "/home/thuantt/airflow/Big_data_project")

from pipeline.crawling_data_pipeline import insert_data_into_database_everyday 

interval = '0 0 * * *'
dag = DAG(dag_id='etl_data',
         start_date=datetime(2024,11,10),
         schedule_interval=interval,
         catchup=True)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag
)

etl_data = PythonOperator(
        task_id='etl_gold_data',
        python_callable=insert_data_into_database_everyday,
        dag=dag)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# Set task dependencies
start_task >> etl_data >> end_task