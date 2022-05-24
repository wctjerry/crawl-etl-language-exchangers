# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MyLanguageExchangeItem(scrapy.Item):
    name = scrapy.Field()
    user_id = scrapy.Field()
    image_url = scrapy.Field()
    last_login = scrapy.Field()
    country = scrapy.Field()
    city = scrapy.Field()
    native_language = scrapy.Field()
    practicing_language = scrapy.Field()
    description = scrapy.Field()
