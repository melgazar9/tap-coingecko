from singer_sdk import typing as th

CUSTOM_JSON_SCHEMA = {
    "additionalProperties": True,
    "description": "Custom JSON typing.",
    "type": ["object", "null"],
}


SUPPORTED_CURRENCIES_SCHEMA = th.PropertiesList(
    th.Property("ticker", th.StringType)
).to_dict()


TOP_GAINERS_LOSERS_SCHEMA = th.PropertiesList(
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


RECENTLY_ADDED_COINS_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("symbol", th.StringType),
    th.Property("name", th.StringType),
    th.Property("activated_at", th.DateTimeType),
).to_dict()


COIN_HISTORICAL_DATA_CHART_BY_ID_SCHEMA = th.PropertiesList(
    th.Property("timestamp", th.DateTimeType),
    th.Property("id", th.StringType),
    th.Property("price", th.NumberType),
    th.Property("market_cap", th.NumberType),
    th.Property("volume", th.NumberType),
).to_dict()

COIN_OHLC_CHART_BY_ID_SCHEMA = th.PropertiesList(
    th.Property("timestamp", th.DateTimeType),
    th.Property("id", th.StringType),
    th.Property("open", th.NumberType),
    th.Property("high", th.NumberType),
    th.Property("low", th.NumberType),
    th.Property("close", th.NumberType),
).to_dict()
