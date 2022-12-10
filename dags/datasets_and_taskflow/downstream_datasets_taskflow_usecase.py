from airflow import DAG
from airflow.decorators import task
from pendulum import datetime
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="downstream_datasets_taskflow_usecase",
    start_date=datetime(2022, 12, 1),
    schedule=None,
    catchup=False
):

    t1 = EmptyOperator(task_id="t1")