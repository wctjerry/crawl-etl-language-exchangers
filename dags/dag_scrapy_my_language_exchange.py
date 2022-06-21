import logging
import os

import pendulum

import constants as c
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from operators.spider_operator import SpiderOperator
from scrapy_utils.spiders.mylanguageexchange import MyLanguageExchangeSpider

logger = logging.getLogger("airflow.task")

SCRAPED_FILE_PATH = os.path.join(
    c.TMP_FILE_PATH,
    "my_language_exchange.csv.gz",
)

ABS_SCRAPED_FILE_PATH = os.path.join(
    c.ABS_ROOT_PATH,
    SCRAPED_FILE_PATH,
)

with DAG(
    dag_id="scrapy_my_language_exchange",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 6, 20, tz="Asia/Shanghai"),
    catchup=False,
    template_searchpath=["dags/sql/language_exchange"],
    tags=["scrapy"],
) as dag:

    scrape_data_task = SpiderOperator(
        task_id="scrape_operator_language_exchange_task",
        spider=MyLanguageExchangeSpider,
        setting={
            "FEEDS": {
                SCRAPED_FILE_PATH: {
                    "format": "csv",
                    "postprocessing": [
                        "scrapy.extensions.postprocessing.GzipPlugin",
                    ],
                    "overwrite": True,
                }
            },
            "DOWNLOAD_DELAY": 2,
            "RANDOMIZE_DOWNLOAD_DELAY": True,
        },
    )

    create_staging = PostgresOperator(
        task_id="create_staging_my_language_exchange",
        sql="create_staging_my_language_exchange.sql",
        params={"tb_name": "staging_my_launguage_exchange"},
        postgres_conn_id="language_exchange_conn",
    )

    load_staging = PostgresOperator(
        task_id="load_staging_my_language_exchange",
        sql="load_staging.sql",
        params={
            "source_path": ABS_SCRAPED_FILE_PATH,
        },
        postgres_conn_id="language_exchange_conn",
    )

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
        >> create_staging
        >> load_staging
        >> etl_dim_users
        >> etl_fct_user_login
    )
