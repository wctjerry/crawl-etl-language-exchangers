import logging
import os

import constants as c
from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scrapy_utils.spiders.mylanguageexchange import MyLanguageExchangeSpider

logger = logging.getLogger("airflow.task")

SCRAPED_FILE_PATH = os.path.join(
    c.TMP_FILE_PATH,
    "my_language_exchange.csv",
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

    def call_scraper_my_language_exchange():
        settings = {
            "FEED_FORMAT": "csv",
            "FEED_URI": SCRAPED_FILE_PATH,
        }

        logger.info("Logging with logger1: started...")
        runner = CrawlerRunner(settings)

        d = runner.crawl(MyLanguageExchangeSpider)
        d.addBoth(lambda _: reactor.stop())
        reactor.run()
        logger.info("Logging with logger1: ended...")

    PythonOperator(
        task_id="scrape_language_exchange_task",
        python_callable=call_scraper_my_language_exchange,
    )

    def test_network():
        import requests

        r = requests.get("https://api.github.com/events")
        if r.status_code == 200:
            logger.info("Network works well!")

    PythonOperator(
        task_id="test_network_task",
        python_callable=test_network,
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

    create_staging >> load_staging
