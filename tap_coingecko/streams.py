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

import pendulum


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
        stream_params = self.config.get("stream_params").get(self.name)
        if stream_params:
            stream_params = urlencode(stream_params)
            endpoint = f"{self.endpoint}?{stream_params}"

        result = requests.get(
            endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
        for record in result.json():
            yield record


class SupportedCurrenciesStream(CoingeckoStream):
    """Coingecko Supported Currencies Stream."""

    name = "supported_currencies"
    path = "/simple/supported_vs_currencies"
    replication_key = None

    schema = SUPPORTED_CURRENCIES_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        result = requests.get(
            self.endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
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
        stream_params = self.config.get("stream_params").get(self.name)
        if stream_params:
            stream_params = urlencode(stream_params)
            endpoint = f"{self.endpoint}?{stream_params}"

        result = requests.get(
            endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
        result = result.json()
        for key in result.keys():
            for record in result[key]:
                record["source"] = key
                yield record


class RecentlyAddedCoinsStream(CoingeckoStream):
    """Coingecko Recently Added Coins Stream."""

    name = "recently_added_coins"
    path = "/coins/list/new"
    replication_key = None

    schema = RECENTLY_ADDED_COINS_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        response = requests.get(
            self.endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
        for record in response.json():
            record["activated_at"] = datetime.fromtimestamp(record["activated_at"])
            yield record


class CoinsListWithMarketDataStream(CoingeckoStream):
    """Coingecko Recently Added Coins Stream."""

    name = "coins_list_with_market_data"
    path = "/coins/markets"
    replication_key = None

    schema = COINS_LIST_WITH_MARKET_DATA_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        stream_params = self.config.get("stream_params").get(self.name)
        self.raise_dynamic_token_ids_not_allowed()

        endpoint = self.endpoint
        if stream_params:
            stream_params = urlencode(stream_params)
            endpoint = f"{self.endpoint}?{stream_params}"

        response = requests.get(
            endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
        for record in response.json():
            yield record


class CoinDataByIdStream(CoingeckoStream):
    """Coingecko Recently Added Coins Stream."""

    name = "coin_data_by_id"
    path = "/coins"
    replication_key = None

    schema = COINS_ID_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        stream_params = self.config.get("stream_params").get(self.name)
        self.raise_dynamic_token_ids_not_allowed()

        endpoint = self.endpoint
        if stream_params:
            encoded_params = urlencode(stream_params)
            endpoint = f"{endpoint}/{stream_params['id']}?{encoded_params}"

        response = requests.get(
            endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
        if response.status_code == 200:
            yield response.json()  # Yield the entire JSON response
        else:
            self.logger.error(
                f"Failed to fetch records from {endpoint}. Status code: {response.status_code}"
            )


class CoinTickersByIdStream(CoingeckoStream):
    """Coingecko Tickers By Id Stream."""

    name = "coin_tickers_by_id"
    path = "/coins"
    replication_key = None

    schema = COIN_TICKERS_BY_ID_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        stream_params = self.config.get("stream_params").get(self.name)
        self.raise_dynamic_token_ids_not_allowed()

        endpoint = f"{self.endpoint}/{stream_params['id']}/tickers"
        if stream_params:
            stream_params.pop("id")
            encoded_params = urlencode(stream_params)
            endpoint = f"{endpoint}?{encoded_params}"

        response = requests.get(
            endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
        yield response.json()


class CoinHistoricalDataByIdStream(CoingeckoStream):
    """Coingecko Historical Data By ID Stream."""

    name = "coin_historical_data_by_id"
    path = "/coins"
    replication_key = None

    schema = COIN_HISTORICAL_DATA_BY_ID_SCHEMA

    def request_records(self, context: dict | None) -> Iterable[dict]:
        stream_params = self.config.get("stream_params").get(self.name)
        self.raise_dynamic_token_ids_not_allowed()

        endpoint = f"{self.endpoint}/{stream_params['id']}/history"
        if stream_params:
            stream_params.pop("id")
            encoded_params = urlencode(stream_params)
            endpoint = f"{endpoint}?{encoded_params}"

        response = requests.get(
            endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
        yield response.json()


class CoinHistoricalDataChartByIdStream(CoingeckoStream):
    """Coingecko Historical Data By ID Stream."""

    name = "coin_historical_data_chart_by_id"
    path = "/coins"
    primary_keys = ["timestamp", "symbol"]
    replication_key = "timestamp"
    is_sorted = True

    schema = COIN_HISTORICAL_DATA_CHART_BY_ID_SCHEMA

    def __init__(
        self,
        tap: Tap,
        name: str | None = None,
        schema: dict[str, t.Any] | Schema | None = None,
        path: str | None = None,
    ):
        super().__init__(tap, name, schema, path)
        self.ticker = None

    @property
    def partitions(self):
        stream_params = self.config.get("stream_params").get(self.name)
        if "ids" in stream_params.keys():
            return [
                {"id": ticker}
                for ticker in [i.strip() for i in stream_params["ids"].split(",")]
            ]
        return []

    def get_url(self, context: dict | None) -> str:
        state = self.get_context_state(context)
        starting_date = self.get_starting_timestamp(context)
        if starting_date:
            starting_date = starting_date.date()
        stream_params = self.config.get("stream_params").get(self.name)

        assert ("id" not in stream_params.keys()) or (
            "ids" not in stream_params.keys()
        ), f"Both 'id' and 'ids' cannot be present in meltano.yml stream params for {self.name}"

        if state and "context" in state.keys() and "id" in state["context"].keys():
            self.ticker = state["context"]["id"]
        else:
            self.ticker = stream_params["id"]

        if starting_date and "days" in stream_params.keys():
            stream_params["days"] = min(
                (datetime.utcnow().date() - starting_date).days, stream_params["days"]
            )

        url_params = stream_params.copy()

        url = f"{self.url_base}{self.path}/{self.ticker}/market_chart"

        if stream_params:
            url_params.pop("ids") if "ids" in url_params else url_params.pop("id")
            encoded_params = urlencode(url_params)
            url = f"{url}?{encoded_params}"

        return url

    def request_records(self, context: dict | None) -> Iterable[dict]:
        url = self.get_url(context)
        response = requests.get(
            url, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )

        result = response.json()

        assert (
            "error" not in result.keys()
        ), f"invalid parameter passed in meltano.yml for coin {context['id']}"

        flattened_result = []

        for price, market_cap, volume in zip(
            result["prices"], result["market_caps"], result["total_volumes"]
        ):
            entry = {
                "timestamp": datetime.utcfromtimestamp(
                    min(price[0] / 1000, market_cap[0] / 1000, volume[0] / 1000)
                ),
                "id": self.ticker,
                "price": price[1],
                "market_cap": market_cap[1],
                "volume": volume[1],
            }
            flattened_result.append(entry)

        for record in flattened_result:
            yield record


class CoinOHLCChartByIdStream(CoingeckoStream):
    """Coingecko Historical Data By ID Stream."""

    name = "coin_ohlc_chart_by_id"
    path = "/coins"
    replication_key = "timestamp"
    primary_keys = ["timestamp", "id"]
    is_sorted = True
    is_timestamp_replication_key = True

    schema = COIN_OHLC_CHART_BY_ID_SCHEMA

    def __init__(
        self,
        tap: Tap,
        name: str | None = None,
        schema: dict[str, t.Any] | Schema | None = None,
        path: str | None = None,
    ):
        super().__init__(tap, name, schema, path)
        self.ticker = None

    @property
    def partitions(self):
        stream_params = self.config.get("stream_params").get(self.name)
        if "ids" in stream_params.keys():
            return [
                {"id": ticker}
                for ticker in [i.strip() for i in stream_params["ids"].split(",")]
            ]
        return []

    def get_url(self, context: dict | None) -> str:
        state = self.get_context_state(context)
        starting_date = self.get_starting_timestamp(context)
        if starting_date:
            starting_date = starting_date.date()
        stream_params = self.config.get("stream_params").get(self.name)

        assert ("id" not in stream_params.keys()) or (
            "ids" not in stream_params.keys()
        ), f"Both 'id' and 'ids' cannot be present in meltano.yml stream params for {self.name}"

        if state and "context" in state.keys() and "id" in state["context"].keys():
            self.ticker = state["context"]["id"]
        else:
            self.ticker = stream_params["id"]

        valid_days = [1, 7, 14, 30, 90, 180, 365, "max"]
        if starting_date and "days" in stream_params.keys():
            min_day = min(
                (datetime.utcnow().date() - starting_date).days, stream_params["days"]
            )
            closest_valid_day = [
                day for day in [i for i in valid_days if i != "max"] if day > min_day
            ]
            if len(closest_valid_day):
                min_valid_day = min(closest_valid_day)
            else:
                min_valid_day = "max"
            stream_params["days"] = min_valid_day

        url_params = stream_params.copy()

        url = f"{self.url_base}{self.path}/{self.ticker}/ohlc"

        if stream_params:
            url_params.pop("ids") if "ids" in url_params else url_params.pop("id")
            encoded_params = urlencode(url_params)
            url = f"{url}?{encoded_params}"

        return url

    def parse_response(self, response):
        result = response.json()
        assert isinstance(
            result, list
        ), f"invalid parameter passed in meltano.yml for coin {context['id']}"

        [r.append(self.ticker) for r in result]

        keys = ["timestamp", "open", "high", "low", "close", "id"]
        data = [dict(zip(keys, values)) for values in result]
        data = [
            {**entry, "timestamp": datetime.utcfromtimestamp(entry["timestamp"] / 1000)}
            for entry in data
        ]

        return data

    def request_records(self, context: dict | None) -> Iterable[dict]:
        url = self.get_url(context)
        response = requests.get(
            url, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )

        result = self.parse_response(response)

        latest_replication_timestamp = datetime.strptime(
            self.get_starting_replication_key_value(context), "%Y-%m-%dT%H:%M:%S%z"
        ).replace(tzinfo=None)
        for record in result:
            if record["timestamp"] >= latest_replication_timestamp:
                yield record
