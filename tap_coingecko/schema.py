from singer_sdk import typing as th

COIN_LIST_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType, description="Coingecko ticker ID"),
    th.Property("symbol", th.StringType, description="Coingecko symbol / ticker"),
    th.Property("name", th.StringType, description="Coingecko product name"),
    th.Property("platforms", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]}), description="Platforms for the token address")
).to_dict()


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
    th.Property("roi", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("last_updated", th.DateTimeType)
).to_dict()

COINS_ID_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("symbol", th.StringType),
    th.Property("name", th.StringType),
    th.Property("web_slug", th.StringType),
    th.Property("asset_platform_id", th.StringType),
    th.Property("platforms", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("detail_platforms", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("block_time_in_minutes", th.NumberType),
    th.Property("hashing_algorithm", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("categories", th.ArrayType(th.StringType)),
    th.Property("preview_listing", th.BooleanType),
    th.Property("public_notice", th.StringType),
    # th.Property("additional_notices", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("localization", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("description", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("links", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("image", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("country_origin", th.StringType),
    th.Property("genesis_date", th.DateTimeType),
    th.Property("sentiment_votes_up_percentage", th.NumberType),
    th.Property("sentiment_votes_down_percentage", th.NumberType),
    th.Property("watchlist_portfolio_users", th.NumberType),
    th.Property("market_cap_rank", th.NumberType),
    th.Property("market_data", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("community_data", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("developer_data", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("status_updates", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("last_updated", th.DateTimeType),
    th.Property("tickers", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]}))
).to_dict()

COIN_TICKERS_BY_ID_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("symbol", th.StringType),
    th.Property("name", th.StringType),
    th.Property("web_slug", th.StringType),
    th.Property("asset_platform_id", th.StringType),
    th.Property("platforms", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("detail_platforms", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("block_time_in_minutes", th.StringType),
    th.Property("hashing_algorithm", th.StringType),
    th.Property("categories", th.ArrayType(th.StringType)),
    th.Property("preview_listing", th.BooleanType),
    th.Property("public_notice", th.StringType),
    th.Property("additional_notices", th.ArrayType(th.StringType)),
    th.Property("localization", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("description", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("links", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("image", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("country_origin", th.StringType),
    th.Property("genesis_date", th.DateTimeType),
    th.Property("contract_address", th.StringType),
    th.Property("sentiment_votes_up_percentage", th.NumberType),
    th.Property("sentiment_votes_down_percentage", th.NumberType),
    th.Property("watchlist_portfolio_users", th.NumberType),
    th.Property("market_cap_rank", th.NumberType),
    th.Property("market_data", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("community_data", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("developer_data", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("status_updates", th.ArrayType(th.StringType)),
    th.Property("last_updated", th.DateTimeType),
    th.Property("tickers", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]}))
).to_dict()


COIN_HISTORICAL_DATA_BY_ID_SCHEMA = th.PropertiesList(
    th.Property("id", th.StringType),
    th.Property("symbol", th.StringType),
    th.Property("name", th.StringType),
    th.Property("localization", th.CustomType({"anyOf": [{"type": "object"}, {"type": "string"}, {}]})),
    th.Property("image", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("market_data", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("community_data", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("developer_data", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]})),
    th.Property("public_interest_stats", th.CustomType({"anyOf": [{"type": "object"}, {"type": "array"}, {}]}))
).to_dict()

COIN_HISTORICAL_DATA_CHART_BY_ID_SCHEMA = th.PropertiesList(
    th.Property("timestamp", th.DateTimeType),
    th.Property("ticker", th.StringType),
    th.Property("price", th.NumberType)
).to_dict()
