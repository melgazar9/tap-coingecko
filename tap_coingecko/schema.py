from singer_sdk import typing as th

COIN_LIST_SCHEMA = th.PropertiesList(
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

SUPPORTED_CURRENCIES_SCHEMA = th.PropertiesList(th.Property("ticker", th.StringType)).to_dict()