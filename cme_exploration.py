import marimo

__generated_with = "0.13.8"
app = marimo.App(width="medium")


@app.cell
def _():
    from coinmetrics.api_client import CoinMetricsClient
    import os
    import marimo as mo
    from datetime import datetime, timedelta, time, timezone
    import matplotlib.pyplot as plt
    import pandas as pd

    client = CoinMetricsClient(os.getenv('CM_API_KEY'))

    end_time = datetime.now(tz=timezone.utc)
    start_time = end_time - timedelta(days=60)
    return client, end_time, pd, start_time


@app.cell
def _(client):
    cme = client.reference_data_markets(exchange='cme').to_dataframe()
    cme
    return (cme,)


@app.cell
def _(cme, end_time):
    cme.loc[(cme.pair=='btc-usd') & (cme.expiration >= end_time)].groupby(['expiration', 'pair', 'type', 'contract_size'], observed=True).agg({
        'strike': ['count', 'min', 'max'],
        'market': ['count', 'first']
    })
    return


@app.cell
def _(cme, end_time):
    my_markets = cme.loc[(cme.symbol.apply(len)==5) 
        & (cme.expiration >= end_time)
        & (cme.size_asset.isin(['btc', 'eth']))
        ].sort_values(['expiration', 'size_asset']).loc[:, ['expiration', 'symbol', 'market']]
    my_markets.set_index('symbol')
    return (my_markets,)


@app.cell
def _(client, my_markets):
    client.catalog_market_candles_v2(markets=list(my_markets.market)).to_dataframe().set_index(['market', 'frequency'])
    return


@app.cell
def _(client, start_time):
    df = client.get_market_candles(markets=['cme-BTCM5-future',  ],
                             start_time=start_time,
                             frequency='1d').to_dataframe() # 'cme-BTCK5-future', 'cme-BTCM5-future',
    df
    return (df,)


@app.cell
def _(df, pd):
    # Ensure we have a datetime index or 'time' column
    if not isinstance(df.index, pd.DatetimeIndex):
        if 'time' in df.columns:
            _df_time = pd.to_datetime(df['time'])
        else:
            raise ValueError("No datetime index or 'time' column found.")
    else:
        _df_time = df.index

    # Cumulative sum of volume per day, starting at midnight
    _df = df.copy()
    _df['__date'] = _df_time.dt.date
    _df['vol_current_day_'] = _df.groupby(['market','__date'])['volume'].cumsum()
    _df = _df.drop(columns='__date')

    _df
    return


@app.cell
def _(df, pd):
    import plotly.graph_objs as go

    candles_90 = df.sort_index().iloc[-90:].copy()
    # If the index is not a DatetimeIndex, reset and use time column if available
    if not isinstance(candles_90.index, pd.DatetimeIndex):
        if 'time' in candles_90.columns:
            candles_90['time'] = pd.to_datetime(candles_90['time'])
        else:
            # fallback to numeric index as x axis
            candles_90['time'] = candles_90.index

    # Candle chart
    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=candles_90.get('time', candles_90.index),
        open=candles_90['price_open'],
        high=candles_90['price_high'],
        low=candles_90['price_low'],
        close=candles_90['price_close'],
        name="Price",
        increasing_line_color='green',
        decreasing_line_color='red',
    ))

    fig.add_trace(go.Bar(
        x=candles_90.get('time', candles_90.index),
        y=candles_90['volume'],
        name='Volume',
        marker_color='steelblue',
        opacity=0.35,
        yaxis='y2',
    ))

    # Layout for dual y-axis
    fig.update_layout(
        title="CME Candle Chart with Volume (BTCM5=June 2025 epxiry)",
        yaxis_title="Price",
        xaxis_title="Time",
        yaxis2=dict(title='Volume', overlaying='y', side='right', showgrid=False),
        xaxis_rangeslider_visible=False,
        height=500,
        legend=dict(orientation='h', yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig
    return


@app.cell
def _(client):
    client.catalog_market_contract_prices_v2(exchange='cme').to_dataframe()
    return


@app.cell
def _(client):
    client.catalog_market_open_interest_v2(exchange='cme').to_dataframe()
    return


@app.cell
def _(client):
    client.catalog_market_implied_volatility_v2(exchange='cme').to_dataframe()
    return


@app.cell
def _(client):
    client.catalog_market_funding_rates_v2(exchange='cme').to_dataframe()
    return


@app.cell
def _(client):
    client.catalog_market_greeks_v2(exchange='cme').to_dataframe()
    return


@app.cell
def _(client, my_markets):
    ob = client.catalog_market_orderbooks_v2(markets=list(my_markets.market),
                             ).to_list()
    ob[0]
    return (ob,)


@app.cell
def _(ob, pd):
    unpack = []

    for m in ob:
        for de in m['depths']:
            unpack.append(
                {
                    "market": m['market'],
                    "depth": de['depth'],
                    "min_time": de['min_time'],
                    "max_time": de['max_time'],
                }
            )
    pd.DataFrame(unpack)

    return


@app.cell
def _(client):
    EAM = client.catalog_exchange_asset_metrics_v2().to_dataframe()
    return (EAM,)


@app.cell
def _(EAM):
    EAM.loc[(EAM.metric.str.contains('future')) & (EAM.exchange_asset.str.startswith('cme-'))]
    return


@app.cell
def _(EAM):
    EAM.loc[(EAM.metric.str.contains('option')) & (EAM.exchange_asset.str.startswith('cme-'))]
    return


@app.cell
def _(client, pd):
    import plotly.express as px

    # Prepare data
    metric_name = "volume_reported_option_notional_usd_1d"
    exchange_asset = "cme-usd"
    freq = "1d"
    series_px = client.get_exchange_asset_metrics(
        metrics=[metric_name], 
        frequency=freq, 
        exchange_assets=[exchange_asset],
    ).to_dataframe().reset_index()
    series_px['time'] = pd.to_datetime(series_px['time'])

    # Find top 3
    top3 = series_px.nlargest(3, metric_name)

    # Plot
    fig_px = px.line(
        series_px,
        x='time',
        y=metric_name,
        title="CME Reported Option Notional Volume (Top 3 Labeled)",
        labels={
            "time": "Time",
            metric_name: "Option Notional Volume (USD)"
        },
        markers=True
    )

    # Add explicit text labels for top 3 values
    for _, row in top3.iterrows():
        fig_px.add_annotation(
            x=row['time'],
            y=row[metric_name],
            text=f"{row['time'].date().isoformat()}/{row[metric_name]/1e6:,.0f} mUSD",
            showarrow=True,
            arrowhead=2,
            ay=-40,
            font=dict(color="crimson", size=13)
        )

    fig_px.update_traces(line_color="indigo", marker=dict(size=7))
    fig_px.update_layout(yaxis_title="Option Notional Volume (USD)", xaxis_title="Time")
    fig_px

    return


if __name__ == "__main__":
    app.run()
