"""coingecko tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_coingecko.streams import *

STREAMS = [
    CoinListStream,
    SupportedCurrenciesStream,
    TopGainersLosersStream,
    RecentlyAddedCoinsStream,
    CoinsListWithMarketDataStream,
    CoinDataByIdStream,
    # CoinTickersByIdStream,
    CoinHistoricalDataByIdStream,
    CoinHistoricalDataChartByIdStream,
    CoinOHLCChartByIdStream,
]


class TapCoingecko(Tap):
    """coingecko tap class."""

    name = "tap-coingecko"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            default="https://pro-api.coingecko.com/api/v3",
            required=True,
            description="Coingecko API URL.",
        ),
        th.Property(
            "api_key",
            th.StringType,
            required=True,
            secret=True,
            description="Coingecko Pro API Secret Key.",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
    ).to_dict()

    def discover_streams(self) -> list[Stream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [stream(tap=self) for stream in STREAMS]


if __name__ == "__main__":
    TapCoingecko.cli()
