import logging

from scrapy.crawler import CrawlerRunner
from twisted.internet import reactor

from airflow.models.baseoperator import BaseOperator

logger = logging.getLogger(__name__)


class SpiderOperator(BaseOperator):
    def __init__(self, spider, setting, **kwargs) -> None:
        super().__init__(**kwargs)
        self.spider = spider
        self.setting = setting

    def execute(self, context):
        logger.info("Going to initiate crawl runner...")

        runner = CrawlerRunner(self.setting)
        d = runner.crawl(self.spider)
        d.addBoth(lambda _: reactor.stop())

        logger.info("Going to run the spider...")

        reactor.run()

        logger.info("Spider closed...")
