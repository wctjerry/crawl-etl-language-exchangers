import logging
import os
from datetime import timedelta

import constants as c
import pendulum
from airflow import DAG
from airflow.operators.sql import SQLThresholdCheckOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators.spider_operator import SpiderOperator
from scrapy_utils.spiders.mylanguageexchange import MyLanguageExchangeSpider

logger = logging.getLogger("airflow.task")

EXPORT_DATA_PATH = os.path.join(
    "s3://",
    c.S3_BUCKET,
    c.EXPORT_DATA_PATH,
    "my_language_exchange_{{ ds }}.csv",
)

ABS_EXPORT_FILE_PATH = os.path.join(
    c.ABS_ROOT_PATH,
    EXPORT_DATA_PATH,
)

with DAG(
    dag_id="scrapy_my_language_exchange_v3",
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2022, 6, 20, tz="Asia/Shanghai"),
    catchup=False,
    template_searchpath=["dags/sql/language_exchange"],
    tags=["scrapy"],
) as dag:

    scrape_data_task = SpiderOperator(
        task_id="scrape_operator_language_exchange_task",
        spider=MyLanguageExchangeSpider,
        setting={
            "DOWNLOAD_DELAY": 2,
            "RANDOMIZE_DOWNLOAD_DELAY": True,
        },
        export_data_path=EXPORT_DATA_PATH,
        aws_conn_id="aws_language-exchange_conn",
    )

    # Extract task
    create_load_staging = PostgresOperator(
        task_id="create_load_staging_my_language_exchange",
        sql="create_and_load_staging.sql",
        params={
            "tb_name": "staging_my_launguage_exchange",
            "source_path": ABS_EXPORT_FILE_PATH,
            "bucket_region": c.S3_BUCKET_REGION,
        },
        postgres_conn_id="language_exchange_conn",
    )

    # Transform task
    etl_staging_dim_users = PostgresOperator(
        task_id="ETL_staging_dim_users",
        sql="create_staging_dim_mle_users.sql",
        postgres_conn_id="language_exchange_conn",
    )

    etl_staging_fct_login = PostgresOperator(
        task_id="ETL_staging_fct_login",
        sql="create_staging_fct_login.sql",
        postgres_conn_id="language_exchange_conn",
    )

    # Check quality task
    check_quality_logins = SQLThresholdCheckOperator(
        task_id="check_quality_logins",
        sql="SELECT COUNT(1) FROM staging_changed_mle_login",
        min_threshold="""
                        SELECT COUNT(1)*0.9 FROM fct_mle_user_login 
                        WHERE login_date = '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -2) }}'::DATE
                        """,
        max_threshold="""
                        SELECT COUNT(1)*1.1 FROM fct_mle_user_login 
                        WHERE login_date = '{{ macros.ds_add(data_interval_start.in_timezone("Asia/Shanghai").to_date_string(), -2) }}'::DATE
                        """,
        conn_id="language_exchange_conn",
    )

    # Load task
    etl_dim_users = PostgresOperator(
        task_id="ETL_dim_users",
        sql="etl_dim_mle_users.sql",
        postgres_conn_id="language_exchange_conn",
    )

    etl_fct_user_login = PostgresOperator(
        task_id="ETL_fct_user_login",
        sql="etl_fct_login.sql",
        postgres_conn_id="language_exchange_conn",
    )

    (
        scrape_data_task
        >> create_load_staging
        >> [etl_staging_dim_users, etl_staging_fct_login]
        >> check_quality_logins
        >> etl_dim_users
        >> etl_fct_user_login
    )
