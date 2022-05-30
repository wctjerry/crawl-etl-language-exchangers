import logging
import os

from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor

import constants as c
import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
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
    dag_id="test_scrapy_operator",
    schedule_interval="@weekly",
    start_date=pendulum.datetime(2022, 4, 28, tz="UTC"),
    catchup=False,
    template_searchpath=["dags/sql/language_exchange"],
    tags=["scrapy"],
) as dag:

    def test_network():
        import requests

        r = requests.get("https://api.github.com/events")
        if r.status_code == 200:
            logger.info("Network works well!")

    PythonOperator(
        task_id="test_network_task",
        python_callable=test_network,
    )

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
            "tb_name": "staging_my_launguage_exchange",
            "source_path": ABS_SCRAPED_FILE_PATH,
        },
        postgres_conn_id="language_exchange_conn",
    )

    scrape_data_task >> create_staging >> load_staging
