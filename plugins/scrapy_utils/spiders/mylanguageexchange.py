import logging
from datetime import datetime, timedelta, timezone
from urllib import parse

import pytz
import scrapy

from scrapy_utils.items import MyLanguageExchangeItem

logger = logging.getLogger("__name__")


class MyLanguageExchangeSpider(scrapy.Spider):
    """A spider to crawl language exchange website: www.mylanguageexchange.com"""

    name = "my_language_exchange_spider"

    def start_requests(self):
        """Generate base request based on the base urls

        Yields:
            scrapy.Request: Scrapy Request of the base url
        """

        urls = [
            "https://www.mylanguageexchange.com/search.asp",
        ]
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_search_options)

        logger.info("Fetching search page finished...")

    def parse_search_options(self, response, rows_per_page=250):
        """Parse search page to generate requests to user search results
        in all countries.
        Sorted by login date (descending).

        Args:
            response (scrapy.Response): response to the request
            rows_per_page (int, optional): rows per page/request. Defaults to 250.

        Yields:
            scrapy.Request: request to the user search results
        """
        country_dict = {}
        countries = response.xpath('//select[contains(@id, "selCountry")]/option')

        for country in countries:
            v = country.xpath("@value").extract_first()
            k = country.xpath("text()").extract_first()
            country_dict[k] = v

        reversed_country_dict = {
            int(v): k for k, v in country_dict.items() if v != "null"
        }

        sort_dict = {}
        sorts = response.xpath("//select[contains(@name, 'selOrder')]/option")

        for sort in sorts:
            v = sort.xpath("@value").extract_first()
            k = sort.xpath("normalize-space(text())").extract_first()
            sort_dict[k] = v

        sort_login = sort_dict["Login Date"]
        logger.info(
            f"Parsing search options finished, total {len(country_dict)} pair(s)..."
        )

        for k, v in country_dict.items():
            if k == "- All -":
                continue

            url = f"https://www.mylanguageexchange.com/search.asp?selX3=null&selX6=null&selCountry={v}&txtCity=&txtAgeMin=&txtAgeMax=&selGender=null&selIsClass=null&selX4=null&selTxtChat=null&selX13=null&selFace=null&txtFName=&txtDesc=&selOrder={sort_login}&txtSrchName=&nRows={rows_per_page}&BtnSubSrch=Search"
            yield scrapy.Request(
                url=url,
                callback=self.parse_search_results,
                meta={
                    "reversed_country_dict": reversed_country_dict,
                },
            )

    def parse_search_results(self, response):
        """Parse user data returned in the search results.

        Args:
            response (scrapy.Response): response to the request

        Yields:
            scrapy.Item: a single user's scraped data
        """
        reversed_country_dict = response.meta["reversed_country_dict"]

        url = response.url
        parameters = parse.parse_qs(parse.urlsplit(url).query)
        country = reversed_country_dict.get(
            int(parameters.get("selCountry", "undefined")[0]), "undefined"
        )
        page = parameters.get("Cnt", [1])[0]
        nrows = parameters.get("nRows", ["undefined"])[0]
        logger.info(f"Scraping country {country} at page {page} with {nrows} rows...")

        rows = response.xpath("//table[contains(@class, 'TblSrchResults')]/tr")

        for row in rows:
            item = MyLanguageExchangeItem()

            name = row.xpath(".//td[contains(@class, 'userdata')]")
            item["name"] = (
                name.xpath(".//a[contains(@href, 'MemberInfo')]/text()")[1]
                .extract()
                .strip()
            )
            item["last_login"] = name.xpath(".//small/font/text()").extract_first()
            item["user_id"] = (
                name.xpath(".//a[contains(@href, 'MemberInfo')]/@href")
                .extract_first()
                .split("=")[-1]
            )
            item["image_url"] = name.xpath(".//img/@src").extract_first()

            location = row.xpath(".//td[contains(@data-th, 'Country(City)')]")
            item["country"] = location.xpath(".//td/div/text()").extract()[0].strip()
            try:
                item["city"] = location.xpath(".//td/div/text()").extract()[1].strip()
            except:
                item["city"] = "not provided"

            native_lang = row.xpath(
                ".//td[contains(@data-th, 'Native Language')]//td/div/text()"
            )
            item["native_language"] = ",".join(i.extract().strip() for i in native_lang)

            practicing_lang = row.xpath(
                ".//td[contains(@data-th, 'Practicing Language')]//td/div/text()"
            )
            item["practicing_language"] = ",".join(
                i.extract().strip() for i in practicing_lang
            )

            description = row.xpath(
                ".//td[contains(@data-th, 'Description')]"
            )  ## Not complete
            item["description"] = description.xpath(
                "normalize-space(.//span/text())"
            ).extract_first()

            yield item

        oldest_last_login_dt = datetime.strptime(item["last_login"], "%B %d, %Y").date()

        target_date = self.get_target_date()
        if oldest_last_login_dt < target_date:
            logger.info(
                f"Stop scraping {country} because {item['last_login']} exceeds cutoff date {target_date.strftime('%B %d, %Y')}"
            )
        else:
            navigations = response.xpath(
                ".//a[contains(@class, 'PageArrow')]/@href"
            ).extract()
            yield response.follow(
                navigations[-1],
                callback=self.parse_search_results,
                meta={
                    "reversed_country_dict": reversed_country_dict,
                },
            )

    def get_target_date(self):
        tz = pytz.timezone("Asia/Shanghai")
        utc_now = datetime.now(timezone.utc)
        local_now = utc_now.astimezone(tz)
        target_date = local_now.date() - timedelta(days=2)  # to avoid time zone difference

        return target_date
