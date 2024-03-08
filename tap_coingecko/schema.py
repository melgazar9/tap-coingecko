from singer_sdk import typing as th

COIN_LIST_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType, description="Coingecko ticker ID"),
    th.Property("symbol", th.StringType, description="Coingecko symbol / ticker"),
    th.Property("name", th.StringType, description="Coingecko product name"),
    th.Property("platforms", th.ObjectType(additional_properties=True), description="Platforms for the token address")
).to_dict()

COIN_LIST_SCHEMA["properties"]["platforms"].pop("properties")  # TODO: use proper th.Property to account for JSON values


SUPPORTED_CURRENCIES_SCHEMA = th.PropertiesList(th.Property("ticker", th.StringType)).to_dict()

TOP_GAINERS_LOSERS_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("source", th.StringType),
    th.Property("symbol", th.StringType),
    th.Property("name", th.StringType),
    th.Property("image", th.StringType),
    th.Property("market_cap_rank", th.NumberType),
    th.Property("usd", th.NumberType),
    th.Property("usd_24h_vol", th.NumberType),
    th.Property("usd_24h_change", th.NumberType)
).to_dict()