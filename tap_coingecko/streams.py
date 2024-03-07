"""Stream type classes for tap-coingecko."""

from __future__ import annotations

import sys
import typing as t
import requests
from singer_sdk import typing as th
from tap_coingecko.client import CoingeckoStream
import importlib.resources as importlib_resources

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

        endpoint = f"{self.config.get('api_url')}{self.path}"
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

        endpoint = f"{self.config.get('api_url')}{self.path}"
        result = requests.get(endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")})
        for record in [{"ticker": value} for value in result.json()]:
            yield record