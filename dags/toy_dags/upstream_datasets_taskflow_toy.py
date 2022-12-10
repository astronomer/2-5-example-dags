from airflow import DAG, Dataset
from airflow.decorators import task
from pendulum import datetime

with DAG(
    dag_id="upstream_datasets_taskflow_toy",
    start_date=datetime(2022, 12, 1),
    schedule=None,
    catchup=False,
    tags=["datasets", "taskflow"]
):

    @task
    def get_folder_name():
        return "pictures_of_Avery"

    # returning a dataset object from a task flow task (2.5 feature) will update the dataset
    # Update 1 to Dataset("my_blob_storage://pictures_of_Avery")
    @task
    def define_dataset(folder_name):
        return Dataset(f"my_blob_storage://{folder_name}")

    my_dataset = define_dataset(get_folder_name())

    # you can pass the call to a task flow task to the outlets parameter
    # Update 2 to Dataset("my_blob_storage://pictures_of_Avery")
    @task(
        outlets=[my_dataset] # before 2.5 only Dataset objects could be passed in
    )
    def say_hi():
        print("hi")

    # the dependency still has to be set explicitely here, because the dataset is
    # not passed as a parameter to the task flow task itself
    my_dataset >> say_hi()

    