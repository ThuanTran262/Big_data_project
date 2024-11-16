from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
from datetime import timedelta

import sys
sys.path.insert(0, "/home/thuantt/airflow/Big_data_project")

from pipeline.model_prediction_pipeline import model_prediction 

interval = '0 2 * * *'
ds = '{{ds}}'
args = {
        'start_date': datetime(2024,1,1),
        'wait_for_downstream': True,
        'depends_on_past': True}

dag = DAG(dag_id='model_prediction_dag2',
         default_args=args,
         schedule_interval=interval,
         catchup=True)

start_task = DummyOperator(
    task_id='start_task',
    dag=dag,
)

predict_data = PythonOperator(
        task_id='predict_data',
        python_callable=model_prediction,
        op_args=[ds],
        dag=dag)

end_task = DummyOperator(
    task_id='end_task',
    dag=dag,
)

# Set task dependencies
start_task >> predict_data >> end_task