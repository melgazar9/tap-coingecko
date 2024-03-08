"""Stream type classes for tap-coingecko."""

from __future__ import annotations

import sys
import typing as t
import requests
from singer_sdk import typing as th
from tap_coingecko.client import CoingeckoStream
import importlib.resources as importlib_resources
from urllib.parse import urlencode
from tap_coingecko.schema import *


class CoinListStream(CoingeckoStream):
    """Coingecko Coin-List Stream of Tickers."""

    name = "coin_list"
    path = "/coins/list"
    replication_key = None

    schema = COIN_LIST_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """
        Request records from REST endpoint(s), returning response records.
        If pagination is detected, pages will be recursed automatically.
        """
        endpoint = self.endpoint
        stream_params = self.config.get('stream_params').get(self.name)
        if stream_params:
            stream_params = urlencode(stream_params)
            endpoint = f"{self.endpoint}?{stream_params}"

        result = requests.get(endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")})
        for record in result.json():
            yield record

class SupportedCurrenciesStream(CoingeckoStream):
    """Coingecko Supported Currencies Stream."""

    name = "supported_currencies"
    path = "/simple/supported_vs_currencies"
    replication_key = None

    schema = SUPPORTED_CURRENCIES_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """
        Request records from REST endpoint(s), returning response records.
        If pagination is detected, pages will be recursed automatically.
        """

        result = requests.get(self.endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")})
        for record in [{"ticker": value} for value in result.json()]:
            yield record

class TopGainersLosersStream(CoingeckoStream):
    """Coingecko Top Gainers / Losers Stream."""

    name = "top_gainers_losers"
    path = "/coins/top_gainers_losers"
    replication_key = None

    schema = TOP_GAINERS_LOSERS_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """
        Request records from REST endpoint(s), returning response records.
        If pagination is detected, pages will be recursed automatically.
        """

        # TODO: Create dynamic schema to account for endpoint parameters in meltano.yml.
        #  Currently this stream only supports USD denominated values.

        endpoint = self.endpoint
        stream_params = self.config.get('stream_params').get(self.name)
        if stream_params:
            stream_params = urlencode(stream_params)
            endpoint = f"{self.endpoint}?{stream_params}"

        result = requests.get(endpoint, headers={"x-cg-pro-api-key": self.config.get('api_key')})
        result = result.json()
        for key in result.keys():
            for record in result[key]:
                record['source'] = key
                yield record