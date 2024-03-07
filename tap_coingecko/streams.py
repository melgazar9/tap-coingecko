"""Stream type classes for tap-coingecko."""

from __future__ import annotations

import sys
import typing as t
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_coingecko.client import CoingeckoStream

if sys.version_info >= (3, 9):
    import importlib.resources as importlib_resources
else:
    import importlib_resources


class CoinListStream(CoingeckoStream):
    """Coingecko Coin-List Stream of Tickers."""

    name = "coin-list"
    path = "/coins/list"
    replication_key = None

    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property(
            "id",
            th.StringType,
            description="Coingecko ticker ID",
        ),
        th.Property(
            "symbol",
            th.StringType,
            description="Coingecko Symbol",
        ),
        th.Property(
            "name",
            th.StringType,
            description="Coingecko ticker name",
        )
    ).to_dict()

    def request_records(self, context: dict | None) -> Iterable[dict]:
        """
        Request records from REST endpoint(s), returning response records.
        If pagination is detected, pages will be recursed automatically.
        """

        state = self.get_context_state(context)
        endpoint = f"{self.config.get('api_url')}{self.path}"
        result = requests.get(endpoint, headers={"x-cg-pro-api-key": self.config.get("api_key")})
        for record in result.json():
            yield record