import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _():
    from coinmetrics.api_client import CoinMetricsClient
    import os
    import marimo as mo
    from datetime import datetime, timedelta, time, timezone
    from dateutil.relativedelta import relativedelta
    import matplotlib.pyplot as plt
    import pandas as pd

    api_key = os.getenv('CM_API_KEY')

    client = CoinMetricsClient(api_key)

    return client, datetime, relativedelta, timedelta, timezone


@app.cell
def _(client, datetime, timedelta, timezone):
    now_utc = datetime.now(timezone.utc)

    x = client.get_market_trades(
        markets=["coinbase-btc-usd-spot"],
        start_time=now_utc-timedelta(seconds=30),
        end_time=now_utc, 
        page_size = 1000
    ).to_dataframe()

    x
    return now_utc, x


@app.cell
def _(now_utc, x):
    latest_time = x['time'].max()
    delay_ms = (now_utc - latest_time).total_seconds() * 1000

    {
        "latest_trade_time": latest_time,
        "now_utc": now_utc,
        "delay_ms": delay_ms
    }

    return


@app.cell
def _(client, now_utc, relativedelta, timedelta):
    y = client.get_market_trades(
        markets=["coinbase-btc-usd-spot"],
        start_time=now_utc-timedelta(hours=24),
        end_time=now_utc, 
        page_size = 10000
    ).parallel(time_increment=relativedelta(minutes=10)).to_dataframe()

    y
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
