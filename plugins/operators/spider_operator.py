import logging

from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor
from urllib.parse import urlparse

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

logger = logging.getLogger(__name__)


class SpiderOperator(BaseOperator):
    template_fields = ("export_data_path",)

    def __init__(
        self, spider, setting, export_data_path, aws_conn_id="", **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.spider = spider
        self.setting = setting
        self.export_data_path = export_data_path
        self.aws_conn_id = aws_conn_id

    def execute(self, context):
        logger.info("Going to initiate crawl runner...")

        self.setting["FEEDS"] = {
            self.export_data_path: {
                "format": "csv",
                "overwrite": True,
            }
        }

        connection = BaseHook.get_connection(self.aws_conn_id)
        self.setting["AWS_ACCESS_KEY_ID"] = connection.login
        self.setting["AWS_SECRET_ACCESS_KEY"] = connection.password

        runner = CrawlerRunner(self.setting)
        d = runner.crawl(self.spider)
        d.addBoth(lambda _: reactor.stop())

        logger.info("Going to run the spider...")

        reactor.run()

        logger.info("Spider closed...")

        parsed_url = urlparse(self.export_data_path, allow_fragments=False)
        bucket = parsed_url.netloc
        object_name = parsed_url.path

        self.xcom_push(context, key="bucket", value=bucket)
        self.xcom_push(context, key="object_name", value=object_name)
        logger.info(f"S3 URI of the scraped data: bucket {bucket}, object name {object_name}")
