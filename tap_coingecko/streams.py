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
from datetime import datetime

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


class RecentlyAddedCoinsStream(CoingeckoStream):
    """Coingecko Recently Added Coins Stream."""

    name = "recently_added_coins"
    path = "/coins/list/new"
    replication_key = None

    schema = RECENTLY_ADDED_COINS_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        response = requests.get(self.endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")})
        for record in response.json():
            record['activated_at'] = datetime.fromtimestamp(record['activated_at'])
            yield record


class CoinsListWithMarketDataStream(CoingeckoStream):
    """Coingecko Recently Added Coins Stream."""

    name = "coins_list_with_market_data"
    path = "/coins/markets"
    replication_key = None

    schema = COINS_LIST_WITH_MARKET_DATA_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        stream_params = self.config.get('stream_params').get(self.name)
        if stream_params and stream_params.get('ids') and stream_params.get('ids') == '*':
            raise NotImplementedError("Cannot set dynamic stream to pull all tickers yet.")

        endpoint = self.endpoint
        if stream_params:
            stream_params = urlencode(stream_params)
            endpoint = f"{self.endpoint}?{stream_params}"

        response = requests.get(endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")})
        for record in response.json():
            yield record


class CoinsDataByIdStream(CoingeckoStream):
    """Coingecko Recently Added Coins Stream."""

    name = "coin_data_by_id"
    path = "/coins"
    replication_key = None

    schema = COINS_ID_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        stream_params = self.config.get('stream_params').get(self.name)
        if stream_params and (stream_params.get('ids') or stream_params.get('id')) and\
                (stream_params.get('id') == '*' or stream_params.get('ids') == '*'):
            raise NotImplementedError("Cannot set dynamic stream to pull all tickers (yet). Can only pull one ticker at a time.")

        endpoint = self.endpoint
        if stream_params:
            encoded_params = urlencode(stream_params)
            endpoint = f"{endpoint}/{stream_params['id']}?{encoded_params}"

        response = requests.get(endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")})
        if response.status_code == 200:
            yield response.json()  # Yield the entire JSON response
        else:
            self.logger.error(f"Failed to fetch records from {endpoint}. Status code: {response.status_code}")
