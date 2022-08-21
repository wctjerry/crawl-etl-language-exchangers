import logging

from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor

from airflow.hooks.base import BaseHook
from airflow.models.baseoperator import BaseOperator

logger = logging.getLogger(__name__)


class SpiderOperator(BaseOperator):
    template_fields = ("export_data_path",)

    def __init__(
        self, spider, setting, export_data_path, aws_conn_id, **kwargs
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

        export_data_path = list(self.setting.get("FEEDS").keys())[0]
        self.xcom_push(context, key="s3_uri", value=export_data_path)
        logger.info(f"S3 URI of the scraped data: {export_data_path}")
