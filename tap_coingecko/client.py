"""REST client handling, including CoingeckoStream base class."""

from __future__ import annotations

import sys
from typing import Any, Callable, Iterable
import requests
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.pagination import BaseAPIPaginator  # noqa: TCH002
from singer_sdk.streams import RESTStream
import importlib.resources as importlib_resources

_Auth = Callable[[requests.PreparedRequest], requests.PreparedRequest]


class CoingeckoStream(RESTStream):
    """coingecko stream class."""

    def raise_dynamic_token_ids_not_allowed(self):
        stream_params = self.config.get("stream_params").get(self.name)
        if (
            stream_params
            and stream_params.get("ids")
            and stream_params.get("ids") == "*"
        ):
            raise NotImplementedError(
                "Cannot set dynamic stream to pull all tickers yet."
            )

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    @property
    def endpoint(self) -> str:
        return f"{self.url_base}{self.path}"

    records_jsonpath = "$[*]"

    # Set this value or override `get_new_paginator`
    next_page_token_jsonpath = "$.next_page"

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object.

        Returns:
            An authenticator instance.
        """
        return APIKeyAuthenticator.create_for_stream(
            self, key="api-key", value=self.config.get("api_key"), location="header"
        )


class DynamicIDCoingeckoStream(CoingeckoStream):
    def __init__(
        self,
        tap: Tap,
        name: str | None = None,
        schema: dict[str, t.Any] | Schema | None = None,
        path: str | None = None,
    ):
        super().__init__(tap, name, schema, path)
        self.ticker = None
        self.stream_params = self.config.get("stream_params").get(self.name)

        assert ("id" not in self.stream_params.keys()) or (
            "ids" not in self.stream_params.keys()
        ), f"Both 'id' and 'ids' cannot be present in meltano.yml stream params for {self.name}"

        self.multi_tickers = False
        if "ids" in self.stream_params.keys():
            self.multi_tickers = True

        self.dynamic_ticker_stream = False
        if "ids" in self.stream_params and self.stream_params["ids"] == "*":
            self.dynamic_ticker_stream = True

        if self.dynamic_ticker_stream:
            coin_list_endpoint = "https://pro-api.coingecko.com/api/v3/coins/list"
            response = requests.get(
                coin_list_endpoint,
                headers={"x-cg-pro-api-key": self.config.get("api_key")},
            )
            self.all_tickers = response.json()

    @property
    def partitions(self):
        if self.multi_tickers:
            if self.stream_params["ids"] != "*":
                return [
                    {"id": ticker}
                    for ticker in [
                        i.strip() for i in self.stream_params["ids"].split(",")
                    ]
                ]
            elif self.stream_params["ids"] == "*":
                return [{"id": t["id"]} for t in self.all_tickers]
            else:
                raise ValueError("Could not set a proper partition.")
        else:
            return [{"id": self.stream_params.get("id")}]
