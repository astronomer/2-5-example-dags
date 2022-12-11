from airflow import DAG
from airflow.decorators import task_group, task
from pendulum import datetime

with DAG(
    dag_id="task_group_mapping_toy",
    start_date=datetime(2022, 12, 1),
    schedule=None,
    catchup=False
):

    # the task group
    @task_group(
        group_id="group2"
    )
    def tg2(my_num):

        @task
        def print_num(num):
            return num
        
        @task
        def add_42(num):
            return num + 42
        
        print_num(my_num) >> add_42(my_num)

    # a downstream task to print out resulting XComs
    @task
    def pull_xcom(**context):

        pulled_xcom = context["ti"].xcom_pull(task_ids=['group2.add_42'], key="return_value")

        # will print out a list of all results from the add_42 task in all mapped
        # task instances of the task group
        print(pulled_xcom)

    # creating 6 mapped task instances of the TaskGroup tg2
    tg_object = tg2.expand(my_num=[19, 23, 42, 8, 7, 108])

    # setting dependencies
    tg_object >> pull_xcom()

    