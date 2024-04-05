"""Stream type classes for tap-coingecko."""

from __future__ import annotations

import sys
import typing as t
import requests
from singer_sdk import typing as th
from tap_coingecko.client import CoingeckoStream, DynamicIDCoingeckoStream
import importlib.resources as importlib_resources
from urllib.parse import urlencode
from datetime import datetime
from singer_sdk import typing as th

CUSTOM_JSON_SCHEMA = {
    "additionalProperties": True,
    "description": "Custom JSON typing.",
    "type": ["object", "null"],
}


class CoinListStream(CoingeckoStream):
    """Coingecko Coin-List Stream of Tickers."""

    name = "coin_list"
    path = "/coins/list"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType, description="Coingecko ticker ID"),
        th.Property("symbol", th.StringType, description="Coingecko symbol / ticker"),
        th.Property("name", th.StringType, description="Coingecko product name"),
        th.Property(
            "platforms",
            th.CustomType(CUSTOM_JSON_SCHEMA),
            description="Coingecko platforms",
        ),
    ).to_dict()

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

    schema = th.PropertiesList(th.Property("ticker", th.StringType)).to_dict()

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

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("source", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("image", th.StringType),
        th.Property("market_cap_rank", th.NumberType),
        th.Property("usd", th.NumberType),
        th.Property("usd_24h_vol", th.NumberType),
        th.Property("usd_24h_change", th.NumberType),
    ).to_dict()

    def request_records(self, context: dict | None) -> Iterable[dict]:
        # TODO: Create dynamic schema to account for endpoint parameters in meltano.yml.
        #  Currently this stream only supports USD denominated values. May be out of scope.

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

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("activated_at", th.DateTimeType),
    ).to_dict()

    def request_records(self, context: dict | None) -> Iterable[dict]:
        response = requests.get(
            self.endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
        for record in response.json():
            record["activated_at"] = datetime.fromtimestamp(record["activated_at"])
            yield record


class CoinsListWithMarketDataStream(CoingeckoStream):
    """Coingecko Recently Added Coins Stream."""

    # TODO: Need to implement pagination.

    name = "coins_list_with_market_data"
    path = "/coins/markets"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("image", th.StringType),
        th.Property("current_price", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("market_cap_rank", th.NumberType),
        th.Property("fully_diluted_valuation", th.NumberType),
        th.Property("total_volume", th.NumberType),
        th.Property("high_24h", th.NumberType),
        th.Property("low_24h", th.NumberType),
        th.Property("price_change_24h", th.NumberType),
        th.Property("price_change_percentage_24h", th.NumberType),
        th.Property("market_cap_change_24h", th.NumberType),
        th.Property("market_cap_change_percentage_24h", th.NumberType),
        th.Property("price_change_percentage_7d_in_currency", th.NumberType),
        th.Property("circulating_supply", th.NumberType),
        th.Property("total_supply", th.NumberType),
        th.Property("max_supply", th.NumberType),
        th.Property("ath", th.NumberType),
        th.Property("ath_change_percentage", th.NumberType),
        th.Property("atl", th.NumberType),
        th.Property("atl_change_percentage", th.NumberType),
        th.Property("atl_date", th.DateTimeType),
        th.Property("ath_date", th.DateTimeType),
        th.Property("last_updated", th.DateTimeType),
        th.Property("roi", th.CustomType(CUSTOM_JSON_SCHEMA)),
    ).to_dict()

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


class CoinDataByIdStream(DynamicIDCoingeckoStream):
    """Coingecko Recently Added Coins Stream."""

    name = "coin_data_by_id"
    path = "/coins"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("web_slug", th.StringType),
        th.Property("asset_platform_id", th.StringType),
        th.Property("block_time_in_minutes", th.NumberType),
        th.Property("hashing_algorithm", th.StringType),
        th.Property("categories", th.ArrayType(th.StringType)),
        th.Property("preview_listing", th.BooleanType),
        th.Property("public_notice", th.StringType),
        th.Property("additional_notices", th.ArrayType(th.StringType)),
        th.Property("country_origin", th.StringType),
        th.Property("genesis_date", th.StringType),
        th.Property("contract_address", th.StringType),
        th.Property("sentiment_votes_up_percentage", th.NumberType),
        th.Property("sentiment_votes_down_percentage", th.NumberType),
        th.Property("watchlist_portfolio_users", th.NumberType),
        th.Property("market_cap_rank", th.NumberType),
        th.Property("last_updated", th.DateTimeType),
        th.Property("tickers", th.StringType),
        th.Property("platforms", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("detail_platforms", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("localization", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("description", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("links", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("image", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("market_data", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("community_data", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("developer_data", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("ico_data", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property(
            "status_updates",
            th.ArrayType(
                th.CustomType(
                    {
                        "anyOf": [
                            {"type": "object"},
                            {"type": "array"},
                            {"type": "null"},
                            {},
                            [{}],
                        ]
                    }
                )
            ),
        ),
    ).to_dict()

    def get_url(self, context: dict | None) -> str:
        state = self.get_context_state(context)
        starting_date = self.get_starting_timestamp(context)

        if starting_date:
            starting_date = starting_date.date()

        if state and "context" in state.keys() and "id" in state["context"].keys():
            self.ticker = state["context"]["id"]
        else:
            self.ticker = self.stream_params["id"]

        if starting_date and "days" in self.stream_params.keys():
            self.stream_params["days"] = min(
                (datetime.utcnow().date() - starting_date).days,
                self.stream_params["days"],
            )

        url_params = self.stream_params.copy()
        url = f"{self.url_base}{self.path}/{self.ticker}"

        if self.stream_params:
            url_params.pop("ids") if "ids" in url_params else url_params.pop("id")
            encoded_params = urlencode(url_params)
            url = f"{url}?{encoded_params}"
        return url

    def request_records(self, context: dict | None) -> Iterable[dict]:
        url = self.get_url(context)
        response = requests.get(
            url, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
        self.logger.info(f" *** Running ticker {self.ticker} ***")
        yield response.json()

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        row["tickers"] = str(row["tickers"])
        return row


class CoinTickersByIdStream(DynamicIDCoingeckoStream):
    """Coingecko Tickers By Id Stream."""

    # TODO: Need to implement pagination.

    name = "coin_tickers_by_id"
    path = "/coins"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType),
        th.Property("tickers", th.StringType),
    ).to_dict()

    def get_url(self, context: dict | None) -> str:
        state = self.get_context_state(context)
        starting_date = self.get_starting_timestamp(context)

        if starting_date:
            starting_date = starting_date.date()

        if state and "context" in state.keys() and "id" in state["context"].keys():
            self.ticker = state["context"]["id"]
        else:
            self.ticker = self.stream_params["id"]

        if starting_date and "days" in self.stream_params.keys():
            self.stream_params["days"] = min(
                (datetime.utcnow().date() - starting_date).days,
                self.stream_params["days"],
            )

        url_params = self.stream_params.copy()
        url = f"{self.url_base}{self.path}/{self.ticker}/tickers"

        if self.stream_params:
            url_params.pop("ids") if "ids" in url_params else url_params.pop("id")
            encoded_params = urlencode(url_params)
            url = f"{url}?{encoded_params}"
        return url

    def request_records(self, context: dict | None) -> Iterable[dict]:
        url = self.get_url(context)

        response = requests.get(
            url, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
        yield response.json()

    def post_process(
        self,
        row: dict,
        context: dict | None = None,  # noqa: ARG002
    ) -> dict | None:
        self.logger.info(f"*** {row} ***")
        if "tickers" in row.keys():
            row["tickers"] = str(row["tickers"])
        return row


class CoinHistoricalDataByIdStream(CoingeckoStream):
    """Coingecko Historical Data By ID Stream."""

    name = "coin_historical_data_by_id"
    path = "/coins"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("symbol", th.StringType),
        th.Property("name", th.StringType),
        th.Property("ico_data", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("localization", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("image", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("market_data", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("community_data", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("developer_data", th.CustomType(CUSTOM_JSON_SCHEMA)),
        th.Property("public_interest_stats", th.CustomType(CUSTOM_JSON_SCHEMA)),
    ).to_dict()

    def request_records(self, context: dict | None) -> Iterable[dict]:
        self.stream_params = self.config.get("stream_params").get(self.name)
        self.raise_dynamic_token_ids_not_allowed()

        endpoint = f"{self.endpoint}/{self.stream_params['id']}/history"
        if self.stream_params:
            encoded_params = urlencode(self.stream_params)
            endpoint = f"{endpoint}?{encoded_params}"

        response = requests.get(
            endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")}
        )
        yield response.json()


class CoinHistoricalDataChartByIdStream(DynamicIDCoingeckoStream):
    """Coingecko Historical Data By ID Stream."""

    name = "coin_historical_data_chart_by_id"
    path = "/coins"
    primary_keys = ["timestamp", "symbol"]
    replication_key = "timestamp"
    is_sorted = True

    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType),
        th.Property("id", th.StringType),
        th.Property("price", th.NumberType),
        th.Property("market_cap", th.NumberType),
        th.Property("volume", th.NumberType),
    ).to_dict()

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

            yield entry


class CoinOHLCChartByIdStream(DynamicIDCoingeckoStream):
    """Coingecko Historical Data By ID Stream."""

    name = "coin_ohlc_chart_by_id"
    path = "/coins"
    replication_key = "timestamp"
    primary_keys = ["timestamp", "id"]
    is_sorted = True
    is_timestamp_replication_key = True

    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType),
        th.Property("id", th.StringType),
        th.Property("open", th.NumberType),
        th.Property("high", th.NumberType),
        th.Property("low", th.NumberType),
        th.Property("close", th.NumberType),
    ).to_dict()

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
