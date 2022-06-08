# Abundantia
[![Lint & Format & Test](https://github.com/Anieca/Abundantia/actions/workflows/lint_format_test.yml/badge.svg?branch=main)](https://github.com/Anieca/Abundantia/actions/workflows/lint_format_test.yml)

Abundantia is a data curation library for cryptocurrency exchanges.

* Provides simple klines curation method from various exchanges.
* Provides a normalized data frame from Exchange API response.


# Instlation
WIP

# Usage

```python
from datetime import datetime

from abundantia.exchanges.bitflyer_client import BitFlyerClient
from abundantia.datastores.s3_client import S3Client


exchange = BitFlyerClient()
datastore = S3Client("crypto-bucket")

symbol = exchange.SYMBOLS.FX_BTC_JPY
interval = 60
start_date = datetime(2022, 5, 1)
end_date = datetime(2022, 5, 2)

key = f"{exchange.NAME}/{symbol.name}/{start_date.strftime()}_{end_date.strftime()}.zip"

klines = exchange.get_klines(exchange.SYMBOLS.FX_BTC_JPY, interval, start_date, end_date)
datastore.dump(key, klines, index=False)

```
