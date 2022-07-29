import os
from datetime import datetime

import constants as c
from airflow.hooks.S3_hook import S3Hook
from airflow.models import DAG
from airflow.operators.python import PythonOperator

FILE_PATH = os.path.join(
    c.TMP_FILE_PATH,
    "my_language_exchange.csv.gz",
)


def upload_to_s3(file_name, key, bucket_name):
    hook = S3Hook("s3_conn")
    is_exist = hook.check_for_key(
        key=key,
        bucket_name=bucket_name,
    )
    if is_exist:
        hook.delete_objects(
            bucket=bucket_name,
            keys=key,
        )

    hook.load_file(
        bucket_name=bucket_name,
        key=key,
        filename=file_name,
    )


def read_from_s3(file_path, key, bucket_name):
    import pandas as pd

    hook = S3Hook("s3_conn")
    file_name = hook.download_file(  # Download to a tmp file name
        local_path=file_path,
        key=key,
        bucket_name=bucket_name,
    )
    full_file_path = os.path.join(
        file_path,
        file_name,
    )
    print(pd.read_csv(full_file_path, compression="gzip").head())


with DAG(
    dag_id="TestS3",
    schedule_interval="@once",
    start_date=datetime(2022, 7, 1),
    catchup=False,
) as dag:
    upload_to_s3 = PythonOperator(
        task_id="UploadFileToSe",
        python_callable=upload_to_s3,
        op_kwargs={
            "file_name": FILE_PATH,
            "key": "data/my_language_exchange.csv.gz",
            "bucket_name": "language-exchange",
        },
    )

    download_from_s3 = PythonOperator(
        task_id="DownloadFromS3",
        python_callable=read_from_s3,
        op_kwargs={
            "file_path": c.TMP_FILE_PATH,
            "key": "data/my_language_exchange.csv.gz",
            "bucket_name": "language-exchange",
        },
    )

    upload_to_s3 >> download_from_s3
