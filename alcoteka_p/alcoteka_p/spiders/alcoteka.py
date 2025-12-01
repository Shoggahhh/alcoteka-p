import json
from collections.abc import AsyncIterator
from json import JSONDecodeError
from typing import Any, Iterable
from urllib.parse import urlencode, urljoin

import scrapy
from scrapy.http import Response
import datetime

BASE_URL = "https://alkoteka.com/web-api/v1/product/"


class AlcotekaSpider(scrapy.Spider):
    name = "alcoteka"

    async def start(self) -> AsyncIterator[Any]:
        base_url = BASE_URL
        params = {
            "city_uuid": "4a70f9e0-46ae-11e7-83ff-00155d026416",
            "page": "1",
            "per_page": "5000",
        }
        url = f"{base_url}?{urlencode(params)}"

        yield scrapy.Request(
            url=url,
            callback=self.parse,
        )

    def parse(self, response: Response, **kwargs: Any) -> Any:
        try:
            data = json.loads(response.text)
        except JSONDecodeError:
            self.logger.error(
                "can`t parse json: %s",
                response.text,
            )
            return

        results = data.get("results", [])
        params = {
            "city_uuid": "4a70f9e0-46ae-11e7-83ff-00155d026416",
        }
        urls = [
            f"{urljoin(BASE_URL, slug.get('slug'))}?{urlencode(params)}"
            for slug in results
        ]

        for url in urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_product,
            )

    def parse_product(self, response: Response, **kwargs: Any) -> Any:
        try:
            detail_data = json.loads(response.text)
        except JSONDecodeError:
            self.logger.error(
                "can't parse detail json: %s",
                response.text,
            )
            return

        if not detail_data.get("success"):
            self.logger.warning("Product response not successful: %s", response.url)
            return

        results = detail_data.get("results", [])
        if not results:
            self.logger.warning("Empty product data: %s", response.url)
            return


        yield {
            "timestamp": datetime.datetime.now(datetime.timezone.utc).timestamp(),
            "RPC": results.get("uuid"),
            "url": urljoin(
                "https://alkoteka.com/product",
                f"{results.get('category', {}).get('slug')}/{response.url.split('/product', 1)[-1]}",
            ),
            "title": {
                results.get("name"): results.get("filter_labels", [])[0].get("title")
                or results.get("filter_labels", [])[3].get("title")
            },
            "marketing_tags": [
                item.get("title") for item in results.get("price_details", [])
            ],
            "brand": results.get("description_blocks", [])[0]
            .get("values", [])[0]
            .get("name"),
            "section": [
                results.get("category").get("name"),
                results.get("category").get("parent").get("name"),
            ],
            "price_data": results.get("price_details", []),
            "stock": {
                "in_stock": results.get("quantity_total", 0) > 0,
                "count": results.get("quantity_total"),
            },
            "assets": {
                "main_image": results.get("image_url"),
            },
            "metadata": {
                "uuid": results.get("uuid"),
                "vendor_code": results.get("vendor_code"),
                "country_code": results.get("country_code"),
                "country_name": results.get("country_name"),
                "__description": results.get("description_blocks"),
            },
        }
