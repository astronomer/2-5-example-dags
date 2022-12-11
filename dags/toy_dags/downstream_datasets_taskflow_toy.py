from airflow import DAG, Dataset
from airflow.decorators import task
from pendulum import datetime

MY_FOLDER = "pictures_of_Avery"
my_dataset = Dataset(f"my_blob_storage://{MY_FOLDER}")

with DAG(
    dag_id="downstream_datasets_taskflow_toy",
    start_date=datetime(2022, 12, 1),
    schedule=[my_dataset],
    catchup=False,
    tags=["datasets", "taskflow"]
):

    # passing the Dataset as an argument to a taskflow task (2.4 feature)
    @task
    def create_new_dataset_from_dataset(my_dataset):
        print(f"This DAG was scheduled on {my_dataset}!")
        dataset_uri = my_dataset.uri
        dataset_extra = my_dataset.extra
        print(f"The URI of this the Dataset is {dataset_uri}")
        print(f"The Extra of this the Dataset is {dataset_extra}")

        blob_storage_name = dataset_uri.split(":")[0]
        new_dataset_uri = f"{blob_storage_name}://even_more_pictures_of_Avery"

        new_dataset = Dataset(new_dataset_uri, dataset_extra)

        # returning a Dataset from the task flow task is both possible and
        # will update the Dataset (2.5 feature)
        # this Dataset will only be visible in the UI if defined "strictly"
        return new_dataset

    create_new_dataset_from_dataset(my_dataset)