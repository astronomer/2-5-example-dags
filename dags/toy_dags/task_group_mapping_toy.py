from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.decorators import task_group
from airflow.operators.empty import EmptyOperator
from pendulum import datetime

with DAG(
    dag_id="task_group_mapping_toy",
    start_date=datetime(2022, 12, 1),
    schedule=None,
    catchup=False
):

    with TaskGroup(group_id="group1") as tg1:
        t1 = EmptyOperator(task_id="t1")
        t2 = EmptyOperator(task_id="t2")

        t1 >> t2


    @task_group(
        group_id="group2"
    )
    def tg2():
        t3 = EmptyOperator(task_id="t3")
        t4 = EmptyOperator(task_id="t4")

        t3 >> t4