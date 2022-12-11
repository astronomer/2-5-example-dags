from airflow import DAG
from airflow.decorators import task_group, task
from pendulum import datetime

with DAG(
    dag_id="task_group_mapping_toy",
    start_date=datetime(2022, 12, 1),
    schedule=None,
    catchup=False,
    tags=["task_group", "toy", "dynamic_task_mapping"]
):

    # the task group
    @task_group(
        group_id="group1"
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

        pulled_xcom = context["ti"].xcom_pull(
            task_ids=['group1.add_42'], # reference a task in a taskgroup with task_group_id.task_id
            map_indexes=[2,3], # only pull xcom from specific mapped task instances (2.5 feature)
            key="return_value"
        )

        # will print out a list of results from map index 2 and 3 of the add_42 task 
        print(pulled_xcom)

    # creating 6 mapped task instances of the TaskGroup tg2 (2.5 feature)
    tg_object = tg2.expand(my_num=[19, 23, 42, 8, 7, 108])

    # setting dependencies
    tg_object >> pull_xcom()

    