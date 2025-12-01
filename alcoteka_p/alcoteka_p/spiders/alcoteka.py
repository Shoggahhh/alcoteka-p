import json
from collections.abc import AsyncIterator
from json import JSONDecodeError
from typing import Any
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

    def parse_product(self, response: Response) -> Any:
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

        category = results.get("category") or {}
        parent = category.get("parent") or {}
        filter_labels = results.get("filter_labels") or []
        price_details = results.get("price_details") or []

        # create title (name)
        name = results.get("name") or ""
        title = name
        extra_parts: list[str] = []

        name_lower = name.lower()

        for label in filter_labels:
            if label.get("filter") == "obem":
                volume = label.get("title")
                if volume and volume.lower() not in name_lower:
                    extra_parts.append(volume)
                break

        for label in filter_labels:
            if label.get("filter") == "cvet":
                colour = label.get("title")
                if colour and colour.lower() not in name_lower:
                    extra_parts.append(colour)
                break

        if extra_parts:
            title = f"{name}, {', '.join(extra_parts)}"

        # check brand
        brand_name = None
        for block in results.get("description_blocks", []):
            if block.get("code") == "brend":
                val = block.get("values") or []
                if val:
                    brand_name = val[0].get("name")
                break

        yield {
            "timestamp": int(datetime.datetime.now(datetime.timezone.utc).timestamp()),
            "RPC": results.get("uuid"),
            "url": urljoin(
                "https://alkoteka.com/product/",
                f"{results.get('category', {}).get('slug')}/{response.url.split('/product', 1)[-1]}",
            ),
            "title": title,
            "marketing_tags": [
                item.get("title") for item in price_details if item.get("title")
            ],
            "brand": brand_name,
            "section": (
                [category.get("name"), parent.get("name")]
                if parent
                else [category.get("name")]
            ),
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
