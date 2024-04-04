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


COIN_TICKERS_BY_ID_SCHEMA = th.PropertiesList(
    th.Property("name", th.StringType),
    th.Property("id", th.StringType),
    th.Property("tickers", th.StringType),
).to_dict()


# # COIN_TICKERS_BY_ID_SCHEMA = th.PropertiesList(
# #     th.Property("name", th.StringType),
# #     th.Property("base", th.StringType),
# # th.Property("target", th.StringType),
# # # th.Property("market", th.AnyType()), # json
# # th.Property("last", th.NumberType),
# # th.Property("volume", th.NumberType),
# # th.Property("cost_to_move_up_usd", th.NumberType),
# # th.Property("cost_to_move_down_usd", th.NumberType),
# # th.Property("converted_last", th.NumberType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # th.Property("base", th.StringType),
# # ).to_dict()
#
#
# # COIN_TICKERS_BY_ID_SCHEMA = th.PropertiesList(
# #     th.Property("id", th.StringType),
# #     th.Property("symbol", th.StringType),
# #     th.Property("name", th.StringType),
# #     th.Property("web_slug", th.StringType),
# #     th.Property("asset_platform_id", th.StringType),
# #     # th.Property("platforms", th.ArrayType(th.AnyType())),
# #     # th.Property("detail_platforms", th.AnyType()),
# #     th.Property("block_time_in_minutes", th.StringType),
# #     th.Property("hashing_algorithm", th.StringType),
# #     th.Property("categories", th.ArrayType(th.StringType)),
# #     th.Property("preview_listing", th.BooleanType),
# #     th.Property("public_notice", th.StringType),
# #     # th.Property("additional_notices", th.ArrayType(th.AnyType())),
# #     th.Property("localization", th.AnyType()),
# #     th.Property("description", th.AnyType()),
# #     th.Property("links", th.AnyType()),
# #     th.Property("image", th.AnyType()),
# #     th.Property("country_origin", th.StringType),
# #     th.Property("genesis_date", th.DateTimeType),
# #     th.Property("contract_address", th.StringType),
# #     th.Property("sentiment_votes_up_percentage", th.NumberType),
# #     th.Property("sentiment_votes_down_percentage", th.NumberType),
# #     th.Property("watchlist_portfolio_users", th.NumberType),
# #     th.Property("market_cap_rank", th.NumberType),
# #     th.Property("market_data", th.AnyType()),
# #     th.Property("community_data", th.AnyType()),
# #     th.Property("developer_data", th.AnyType()),
# #     # th.Property("status_updates", th.ArrayType(th.AnyType())),
# #     th.Property("last_updated", th.DateTimeType),
# # ).to_dict()


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
