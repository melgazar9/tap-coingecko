"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import os
from singer_sdk.testing import get_tap_test_class

from tap_coingecko.tap import TapCoingecko

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "api_url": os.getenv("COINGECKO_API_URL"),
    "api_key": os.getenv("COINGECKO_API_KEY"),
}


# Run standard built-in tap tests from the SDK:
TestTapCoingecko = get_tap_test_class(
    tap_class=TapCoingecko,
    config=SAMPLE_CONFIG,
)
