"""Fetch and manage historical exchange rates via ECB API.

European Central Bank (ECB) provides free historical daily exchange rates:
https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml

Rates are EUR to various currencies; we invert to get rates vs USD.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from urllib.request import urlopen

ECB_DAILY_RATE_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml"
ECB_HISTORICAL_RATE_URL = "https://www.ecb.europa.eu/stats/eurofxref/eurofxref-hist.xml"


def load_exchange_rates(
    conn,
    lookback_days: int = 3650,
) -> None:
    """Load historical exchange rates from ECB into the local store.

    Args:
        lookback_days: How far back to fetch rates (default 10 years).

    Fetches daily rates from ECB API (EUR-based), converts to USD-based rates,
    and inserts into exchange_rates table. ECB rates are EUR to other currencies;
    we store "from_currency to USD" for universe aggregations.
    """
    from xml.etree import ElementTree as ET

    cutoff_date = datetime.now().date() - timedelta(days=lookback_days)

    print(f"Fetching ECB exchange rates from {cutoff_date}...", flush=True)
    with urlopen(ECB_HISTORICAL_RATE_URL, timeout=60) as response:
        root = ET.parse(response).getroot()

    rate_rows = []
    namespaces = {"eurofxref": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}

    date_cubes = root.findall(".//eurofxref:Cube[@time]", namespaces)

    for date_cube in date_cubes:
        date_str = date_cube.get("time")
        if date_str is None:
            continue

        try:
            rate_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            if rate_date < cutoff_date:
                continue
        except ValueError:
            continue

        rate_cubes = date_cube.findall("eurofxref:Cube", namespaces)
        eur_to_usd = None
        rates_by_currency = {}

        for rate_cube in rate_cubes:
            currency = rate_cube.get("currency")
            rate_str = rate_cube.get("rate")

            if currency is None or rate_str is None:
                continue

            try:
                rate = float(rate_str)
                rates_by_currency[currency] = rate

                if currency == "USD":
                    eur_to_usd = rate
            except (ValueError, TypeError):
                continue

        if eur_to_usd is None:
            continue

        rate_rows.append(
            {
                "rate_date": rate_date,
                "from_currency": "USD",
                "to_currency": "USD",
                "rate": 1.0,
            }
        )
        rate_rows.append(
            {
                "rate_date": rate_date,
                "from_currency": "EUR",
                "to_currency": "USD",
                "rate": eur_to_usd,
            }
        )

        for currency, eur_to_currency in rates_by_currency.items():
            if currency == "USD":
                continue

            try:
                rate_foreign_to_usd = eur_to_usd / eur_to_currency if eur_to_currency != 0 else 1.0
                rate_rows.append(
                    {
                        "rate_date": rate_date,
                        "from_currency": currency,
                        "to_currency": "USD",
                        "rate": rate_foreign_to_usd,
                    }
                )
            except (ZeroDivisionError, TypeError):
                continue

    if not rate_rows:
        print("No exchange rates found in ECB data.")
        return

    print(f"Inserting {len(rate_rows)} exchange rate records into exchange_rates...", flush=True)

    rows_as_tuples = [(r["rate_date"], r["from_currency"], r["to_currency"], r["rate"]) for r in rate_rows]

    from openbb_sec.db.schema import DATABASE_NAME  # pylint: disable=import-outside-toplevel

    conn.executemany(
        f"INSERT INTO {DATABASE_NAME}.exchange_rates (rate_date, from_currency, to_currency, rate) VALUES (?, ?, ?, ?)",
        rows_as_tuples,
    )
    print("Exchange rates loaded.", flush=True)
