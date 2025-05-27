import marimo

__generated_with = "0.12.9"
app = marimo.App(width="medium")


@app.cell
def _():
    from coinmetrics.api_client import CoinMetricsClient
    import os
    import marimo
    from datetime import datetime, timedelta, time
    import matplotlib.pyplot as plt
    import pandas as pd

    client = CoinMetricsClient(os.getenv('CM_API_KEY'))
    return (
        CoinMetricsClient,
        client,
        datetime,
        marimo,
        os,
        pd,
        plt,
        time,
        timedelta,
    )


@app.cell
def _(client):
    cme = client.reference_data_markets(exchange='cme').to_dataframe()
    cme
    return (cme,)


@app.cell
def _(cme):
    cme.loc[(cme.pair=='btc-usd') & (cme.expiration >= '2025-05-01')].groupby(['expiration', 'pair', 'type', 'contract_size'], observed=True).agg({
        'strike': ['count', 'min', 'max'],
        'market': ['count', 'first']
    })
    return


@app.cell
def _(cme):
    import numpy as np

    # Filter for contract_size == 5
    _cme = cme[cme.contract_size == 5]

    # Only options have a strike price
    # Keep only rows with a strike price
    _cme_opt = _cme[_cme['strike'].notnull()]

    # Prepare grid: rows = expiration, cols = strike
    pivot = (_cme_opt
             .assign(exists=1)
             .pivot_table(index='expiration', columns='strike', values='exists', fill_value=0)
            )

    # Convert to display characters
    display_grid = pivot.applymap(lambda x: '( )' if x == 1 else '-')

    # Sort index (expiration) and columns (strike)
    display_grid = display_grid.sort_index().sort_index(axis=1)

    display_grid


    return display_grid, np, pivot


@app.cell
def _(pd, pivot):
    def _():
        import matplotlib.pyplot as plt

        # Prepare data for plotting
        _grid = pivot.copy()
        _grid.index = pd.to_datetime(_grid.index).date  # Use just date, not full datetime
        strikes = _grid.columns.astype(str)
        expirations = _grid.index.astype(str)

        fig, ax = plt.subplots(figsize=(min(18, 0.38*len(strikes)), min(8, 0.7*len(expirations))))

        # Plot as heatmap (1=option exists, 0=not)
        cax = ax.imshow(_grid.values, aspect='auto', cmap='Greens', interpolation='nearest')

        # Set axis labels and ticks
        ax.set_xticks(range(len(strikes)))
        ax.set_yticks(range(len(expirations)))
        ax.set_xticklabels(strikes, rotation=90, fontsize=10)
        ax.set_yticklabels(expirations, fontsize=10)

        ax.set_xlabel('Strike Price')
        ax.set_ylabel('Expiration Date')
        ax.set_title('CME BTC/ETH Option Grid (contract size = 5)\nGreen: option available')

        # Color bar
        fig.colorbar(cax, ax=ax, fraction=0.036, pad=0.04, label="Option Exists")

        plt.tight_layout()
        return plt.gca()


    _()
    return


@app.cell
def _(cme):
    cme.loc[(cme.pair=='btc-usd') & (cme.expiration == '2025-05-30T15:00:00.000Z') & (cme.type=='future')]
    return


@app.cell
def _(cme):
    cme.loc[[1867, 2473], :]
    return


@app.cell
def _(cme):
    my_markets = cme.loc[(cme.symbol.apply(len)==5) 
        & (cme.expiration >= '2025-05-01')
        & (cme.size_asset.isin(['btc', 'eth']))
        ].sort_values(['expiration', 'size_asset']).loc[:, ['expiration', 'symbol', 'market']]
    my_markets.set_index('symbol')
    return (my_markets,)


@app.cell
def _(client, my_markets):
    client.catalog_market_candles_v2(markets=list(my_markets.market)).to_dataframe().set_index(['market', 'frequency'])
    return


@app.cell
def _(client):
    df = client.get_market_candles(markets=['cme-BTCK5-future',  ],
                             start_time='2025-05-27',
                             frequency='1m').to_dataframe() # 'cme-BTCK5-future', 'cme-BTCM5-future',
    df
    return (df,)


@app.cell
def _():
    # Let's now see if we can replicate the volume calculation on the CME website. 
    # https://www.cmegroup.com/markets/cryptocurrencies/bitcoin/bitcoin.quotes.html
    return


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

    # Ensure column names for price
    price_cols = [col for col in df.columns if col.startswith('price_')]
    # Ensure required candle columns
    required_cols = ['price_open', 'price_close', 'price_low', 'price_high', 'volume']
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Missing required candle columns in df: {required_cols}")

    # Get last 90 minutes of data (assuming 1m frequency)
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
        title="CME Candle Chart with Volume (Last 90 Minutes)",
        yaxis_title="Price",
        xaxis_title="Time",
        yaxis2=dict(title='Volume', overlaying='y', side='right', showgrid=False),
        xaxis_rangeslider_visible=False,
        height=500,
        legend=dict(orientation='h', yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    fig
    return candles_90, fig, go, price_cols, required_cols


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

    return de, m, unpack


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


if __name__ == "__main__":
    app.run()
