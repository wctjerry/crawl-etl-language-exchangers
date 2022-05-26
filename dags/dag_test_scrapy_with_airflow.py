import logging

from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scrapy_utils.spiders.mylanguageexchange import MyLanguageExchangeSpider

logger = logging.getLogger("airflow.task")

with DAG(
    dag_id="test_scrapy_operator",
    schedule_interval="@weekly",
    start_date=pendulum.datetime(2022, 4, 28, tz="UTC"),
    catchup=False,
    tags=["scrapy"],
) as dag:

    def call_scraper_my_language_exchange():
        settings = {
            "FEED_FORMAT": "csv",
            "FEED_URI": "logs/tmp/my_language_exchange.csv",
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

    PostgresOperator(
        task_id="test_postgresql_query",
        sql="""
        DROP TABLE IF EXISTS staging_table;
        CREATE TABLE IF NOT EXISTS staging_table (city VARCHAR,
                                                  country VARCHAR,
                                                  description VARCHAR,
                                                  image_url VARCHAR,
                                                  last_login DATE,
                                                  name VARCHAR,
                                                  native_language VARCHAR,
                                                  practicing_language VARCHAR,
                                                  user_id BIGINT);
        COPY staging_table FROM '/Users/wctjerry/Airflow/logs/tmp/my_language_exchange.csv' DELIMITER ',' CSV HEADER;
        """,
        postgres_conn_id="local_udemy",
    )
