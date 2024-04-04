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
