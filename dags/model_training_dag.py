from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime
from datetime import timedelta

import sys
sys.path.insert(0, "/home/thuantt/airflow/Big_data_project")

from pipeline.model_training_pipeline import model_training 

interval = '0 1 1 * *'
ds = '{{ds}}'
args = {
        'start_date': datetime(2024,1,1),
        'wait_for_downstream': True,
        'depends_on_past': True}

dag = DAG(dag_id='model_training_dag',
         default_args=args,
         schedule_interval=interval,
         catchup=True)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

train_model = PythonOperator(
        task_id='model_training',
        python_callable=model_training,
        op_args=[ds],
        dag=dag)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# Set task dependencies
start_task >> train_model >> end_task