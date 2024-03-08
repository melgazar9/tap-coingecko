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

RECENTLY_ADDED_COINS_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("symbol", th.StringType),
    th.Property("name", th.StringType),
    th.Property("activated_at", th.DateTimeType)
).to_dict()


COINS_LIST_WITH_MARKET_DATA_SCHEMA = th.PropertiesList(
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
    th.Property("circulating_supply", th.NumberType),
    th.Property("total_supply", th.NumberType),
    th.Property("max_supply", th.NumberType),
    th.Property("ath", th.NumberType),
    th.Property("ath_change_percentage", th.NumberType),
    th.Property("atl", th.NumberType),
    th.Property("atl_change_percentage", th.NumberType),
    th.Property("atl_date", th.DateTimeType),
    th.Property("roi", th.ObjectType(additional_properties=True)),
    th.Property("last_updated", th.DateTimeType)
).to_dict()

COINS_LIST_WITH_MARKET_DATA_SCHEMA["properties"]["roi"].pop("properties")  # TODO: use proper th.Property to account for JSON values

COINS_ID_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("symbol", th.StringType),
    th.Property("name", th.StringType),
    th.Property("web_slug", th.StringType),
    th.Property("asset_platform_id", th.StringType),
    th.Property("platforms", th.ObjectType(additional_properties=True)),
    th.Property("detail_platforms", th.ObjectType(additional_properties=True)),
    th.Property("block_time_in_minutes", th.NumberType),
    th.Property("hashing_algorithm", th.ArrayType(th.StringType)),
    th.Property("categories", th.ArrayType(th.StringType)),
    th.Property("preview_listing", th.BooleanType),
    th.Property("public_notice", th.StringType),
    th.Property("additional_notices", th.ArrayType(th.StringType)),
    th.Property("localization", th.ObjectType(additional_properties=True)),
    th.Property("description", th.ObjectType(additional_properties=True)),
    th.Property("links", th.ObjectType(additional_properties=True)),
    th.Property("image", th.ObjectType(additional_properties=True)),
    th.Property("country_origin", th.StringType),
    th.Property("genesis_date", th.DateTimeType),
    th.Property("sentiment_votes_up_percentage", th.NumberType),
    th.Property("sentiment_votes_down_percentage", th.NumberType),
    th.Property("watchlist_portfolio_users", th.NumberType),
    th.Property("market_cap_rank", th.NumberType),
    th.Property("market_data", th.ObjectType(additional_properties=True)),
    th.Property("community_data", th.ObjectType(additional_properties=True)),
    th.Property("developer_data", th.ObjectType(additional_properties=True)),
    th.Property("status_updates", th.ArrayType(th.ObjectType(additional_properties=True))),
    th.Property("last_updated", th.DateTimeType),
    th.Property("tickers", th.ArrayType(th.ObjectType(additional_properties=True)))
).to_dict()

# TODO: use proper th.Property to account for JSON values
for json_column in ["status_updates", "tickers"]:
    COINS_ID_SCHEMA["properties"][json_column].pop("items")

for json_column in ["platforms", "detail_platforms", "localization", "description", "links", "image", "market_data",
                    "community_data", "developer_data"]:
    COINS_ID_SCHEMA["properties"][json_column].pop("properties")
