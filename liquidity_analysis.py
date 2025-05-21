import marimo

__generated_with = "0.13.8"
app = marimo.App(width="medium")


@app.cell
def _():
    from coinmetrics.api_client import CoinMetricsClient
    import os
    import marimo as mo
    from datetime import datetime, timedelta, time
    import matplotlib.pyplot as plt
    import pandas as pd

    client = CoinMetricsClient(os.getenv('CM_API_KEY'))
    return client, mo, pd


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Let's find out which market and exchange-asset metrics are available. """)
    return


@app.cell
def _(client):
    ref = client.reference_data_market_metrics().to_dataframe()
    ref
    return


@app.cell
def _(client):
    ref2 = client.reference_data_exchange_asset_metrics().to_dataframe()
    ref2
    return (ref2,)


@app.cell
def _(ref2):
    ref2.loc[ref2.metric.str.contains('volume_reported_spot')]
    return


@app.cell
def _():
    interesting = ['liquidity_slippage_1M_ask_percent', 'liquidity_slippage_1M_bid_percent',
                  'liquidity_slippage_1K_ask_percent', 'liquidity_slippage_1K_bid_percent',
                  ]

    return (interesting,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""#### Let's combine the reference data for the markets with the catalogue data for the market metrics. """)
    return


@app.cell
def _(client):
    markets = client.reference_data_markets().to_dataframe()
    mmc = client.catalog_market_metrics_v2(base='btc', market_type='spot').to_dataframe().merge(markets)
    return (mmc,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""#### Let's choose some sensible quote currencies. Frequency=1h. """)
    return


@app.cell
def _(interesting, mmc):
    y = mmc.loc[(mmc.quote.isin(['usd', 'usdt', 'usdc', 'fdusd']))
        &(mmc.frequency=='1h')
        &(mmc.metric.isin(interesting))
        ]
    y
    return (y,)


@app.cell
def _(y):
    x = y.groupby(['exchange'], observed=True).size()
    x
    return (x,)


@app.cell
def _(x):
    # get the volume_reported_spot_usd_1h for the exchanges above (for BTC)
    relevant = [f"{e}-btc" for e in x.index]
    relevant
    return (relevant,)


@app.cell
def _(relevant):
    sanitized = list(set(relevant)-{'mexc-btc'})
    return


@app.cell
def _(client, relevant):
    vol = client.get_exchange_asset_metrics(
        metrics='volume_reported_spot_usd_1h',
        exchange_assets=relevant,
        start_time='2025-04-01',
        frequency='1h'
    ).parallel().to_dataframe()
    vol
    return (vol,)


@app.cell
def _(volume_totals):
    # Create a DataFrame with total volume and cumulative sum
    volume_table = volume_totals.reset_index().rename(
        columns={'exchange_asset': 'Exchange Asset', 'volume_reported_spot_usd_1h': 'Total Volume'}
    )
    volume_table['Cumulative Volume'] = volume_table['Total Volume'].cumsum() / volume_table['Total Volume'].sum()
    volume_table.reset_index()

    return


@app.cell
def _(vol):
    # Calculate total volume per exchange asset
    volume_totals = vol.groupby('exchange_asset')['volume_reported_spot_usd_1h'].sum().sort_values(ascending=False)

    # Identify top N exchange assets
    top10 = volume_totals.head(10).index.tolist()

    # Label others as 'other'
    vol_top10 = vol.copy()
    vol_top10['exchange_asset_grouped'] = vol_top10['exchange_asset'].where(
        vol_top10['exchange_asset'].isin(top10), 'other'
    )

    # Group by time and the grouped label, sum volume for others
    vol_hourly_grouped = (
        vol_top10.groupby(['time', 'exchange_asset_grouped'], as_index=False)['volume_reported_spot_usd_1h']
        .sum()
        .rename(columns={'exchange_asset_grouped': 'exchange_asset'})
    )

    vol_hourly_grouped

    return vol_hourly_grouped, vol_top10, volume_totals


@app.cell
def _():
    return


@app.cell
def _(vol_top10):
    import plotly.graph_objects as go

    # Prepare weekly grouped data
    vol_weekly_grouped = vol_top10.copy()
    vol_weekly_grouped['week'] = vol_weekly_grouped['time'].dt.to_period('W').apply(lambda r: r.start_time)
    weekly_vol = (
        vol_weekly_grouped.groupby(['week', 'exchange_asset_grouped'], as_index=False)['volume_reported_spot_usd_1h']
        .sum()
        .rename(columns={'exchange_asset_grouped': 'exchange_asset'})
    )

    # Get unique exchange assets (including 'other') to stack
    exchange_assets = weekly_vol['exchange_asset'].unique()
    weeks = sorted(weekly_vol['week'].unique())

    # Create traces for each exchange asset
    fig_week = go.Figure()
    for asset in exchange_assets:
        asset_data = weekly_vol[weekly_vol['exchange_asset'] == asset]
        fig_week.add_trace(go.Bar(
            x=asset_data['week'],
            y=asset_data['volume_reported_spot_usd_1h'],
            name=asset
        ))

    fig_week.update_layout(
        barmode='stack',
        title='Exchange Asset BTC Spot USD-Quoted Weekly Reported Volumes (Stacked)',
        xaxis_title='Week',
        yaxis_title='Volume (USD)',
        legend_title_text='Exchange Asset',
        xaxis=dict(type='date')
    )

    fig_week

    return (go,)


@app.cell
def _(vol_hourly_grouped):
    import plotly.express as px

    fig_vol = px.line(
        vol_hourly_grouped,
        x="time",
        y="volume_reported_spot_usd_1h",
        color="exchange_asset",
        title="Exchange Asset BTC Spot USD-Quoted 1-Hour Reported Volumes",
        labels={
            "time": "Time",
            "volume_reported_spot_usd_1h": "Volume (USD)",
            "exchange_asset": "Exchange Asset"
        }
    )
    fig_vol.update_layout(
        legend_title_text='Exchange Asset',
        legend=dict(
            orientation='v',
            yanchor='top',
            y=1,
            xanchor='left',
            x=1.02
        )
    )
    fig_vol

    return (px,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
    ## Market Metrics for Slippage 

    Let's download the aggregated slippage metrics for the markets available. Some markets are not deep enough for the 1M metric, so we need to download in groups.
    """
    )
    return


@app.cell
def _(client, pd, y):
    results_fast = []
    for metrics, _markets in y.groupby(['metric']):
        df = client.get_market_metrics(
            metrics=metrics,
            markets=list(_markets.market),
            start_time='2025-04-01',
            frequency='1h'
        ).parallel().to_dataframe()
        results_fast.append(df)

    from functools import reduce

    keys = ['market', 'time']

    merged_df = reduce(lambda left, right: pd.merge(left, right, on=keys, how='left'), results_fast)
    return (merged_df,)


@app.cell
def _(merged_df):
    z = merged_df.copy()
    z
    return (z,)


@app.cell
def _(px, z):
    # Scatterplot: Liquidity Slippage (1K USD) - Ask Side
    fig_ask_scatter = px.scatter(
        z,
        x="time",
        y="liquidity_slippage_1K_ask_percent",
        color="market",
        title="Liquidity Slippage (1K USD) - Ask Side Over Time (Scatter)",
        labels={
            "time": "Time",
            "liquidity_slippage_1K_ask_percent": "Ask Slippage (%)",
            "market": "Market"
        },
        opacity=0.3
    )
    fig_ask_scatter.update_layout(legend_title_text='Market')

    # Scatterplot: Liquidity Slippage (1K USD) - Bid Side
    fig_bid_scatter = px.scatter(
        z,
        x="time",
        y="liquidity_slippage_1K_bid_percent",
        color="market",
        title="Liquidity Slippage (1K USD) - Bid Side Over Time (Scatter)",
        labels={
            "time": "Time",
            "liquidity_slippage_1K_bid_percent": "Bid Slippage (%)",
            "market": "Market"
        },
        opacity=0.3
    )
    fig_bid_scatter.update_layout(legend_title_text='Market')

    (fig_ask_scatter, fig_bid_scatter)

    return


@app.cell
def _(px, z):
    # Scatterplot: Liquidity Slippage (1M USD) - Ask Side
    fig_ask_scatter_m = px.scatter(
        z,
        x="time",
        y="liquidity_slippage_1M_ask_percent",
        color="market",
        title="Liquidity Slippage (1M USD) - Ask Side Over Time (Scatter)",
        labels={
            "time": "Time",
            "liquidity_slippage_1M_ask_percent": "Ask Slippage (%)",
            "market": "Market"
        },
        opacity=0.4
    )
    fig_ask_scatter_m.update_layout(legend_title_text='Market')

    # Scatterplot: Liquidity Slippage (1M USD) - Bid Side
    fig_bid_scatter_m = px.scatter(
        z,
        x="time",
        y="liquidity_slippage_1M_bid_percent",
        color="market",
        title="Liquidity Slippage (1M USD) - Bid Side Over Time (Scatter)",
        labels={
            "time": "Time",
            "liquidity_slippage_1M_bid_percent": "Bid Slippage (%)",
            "market": "Market"
        },
        opacity=0.4
    )
    fig_bid_scatter_m.update_layout(legend_title_text='Market')

    (fig_ask_scatter_m, fig_bid_scatter_m)

    return


@app.cell
def _(z):
    # Approach for ranking markets by lowest average ask slippage (1M), penalizing missing values

    # We'll calculate, for each market:
    # - The average of liquidity_slippage_1M_ask_percent, treating NaNs as a penalty.
    #   For penalty, we assign a high fake slippage (e.g., 10x the max observed slippage)

    # Prepare slippage data per market
    metric = "liquidity_slippage_1M_ask_percent"
    _slippage = z[["market", metric]].copy()

    # Find max observed value to set the penalty for NaNs
    max_slippage = _slippage[metric].max()
    penalty_value = 10 * max_slippage

    def penalized_mean(series, penalty):
        # Replace NaNs with penalty and take mean
        return series.fillna(penalty).mean()

    # Compute penalized averages for all markets
    ranking = (
        _slippage.groupby("market")[metric]
        .apply(lambda s: penalized_mean(s, penalty_value))
        .sort_values()
        .reset_index()
        .rename(columns={metric: "avg_slippage_with_penalty"})
    )

    top10_low_slippage = ranking.head(10)
    top10_low_slippage

    return (top10_low_slippage,)


@app.cell
def _(client, pd, top10_low_slippage):
    # Get today's date (UTC) and 30 days ago
    import numpy as np

    end_date = pd.Timestamp.utcnow().normalize()
    start_date = end_date - pd.Timedelta(days=30)

    # List of top 10 markets (from `top10_low_slippage`)
    top10_market_list = top10_low_slippage['market'].tolist()

    # Fetch daily candles for these markets using CoinMetricsClient
    # We'll use parallel queries to speed up
    candles = client.get_market_candles(
        markets=top10_market_list,
        frequency='1d',
        start_time=start_date.strftime('%Y-%m-%d'),
        end_time=end_date.strftime('%Y-%m-%d'),
    ).parallel().to_dataframe()

    # Aggregate total USD volume per market
    market_30d_volume = (
        candles.groupby('market')['candle_usd_volume']
        .sum()
        .reset_index()
    )

    # Merge with slippage ranking
    slippage_vs_volume = pd.merge(
        top10_low_slippage,
        market_30d_volume,
        on='market',
        how='left'
    )


    return candles, np, slippage_vs_volume


@app.cell
def _(np, px, slippage_vs_volume):
    # Scatter plot: X = avg_slippage_with_penalty, Y = total_30d_usd_volume, text = market
    #import plotly.express as px

    fig_scatter = px.scatter(
        slippage_vs_volume,
        x='avg_slippage_with_penalty',
        y='candle_usd_volume',
        text='market',
        title='Top 10 Markets: Penalized Average Slippage vs. Total 30-day USD Volume',
        labels={
            "avg_slippage_with_penalty": "Avg. Slippage (1M Ask %, penalized)",
            "total_30d_usd_volume": "Total 30d USD Volume",
            "market": "Market"
        },
        size=np.clip(slippage_vs_volume['candle_usd_volume'], 1e6, None),  # marker size reflects volume
        hover_name='market',
        color='market'
    )
    fig_scatter.update_traces(textposition='top right')
    fig_scatter.update_yaxes(type='log')  # log scale for volume for better spread
    fig_scatter.update_layout(showlegend=False)

    fig_scatter
    return


@app.cell
def _(slippage_vs_volume):
    slippage_vs_volume
    return


@app.cell
def _(candles):
    candles.columns
    return


@app.cell
def _(client, pd):
    # Get the metric spelling for trusted spot volume (from candles.columns or doc)
    trusted_metric = "volume_trusted_spot_usd_1h"

    # Get today's date (UTC) and 30 days ago, if not already set
    end_date_pairs = pd.Timestamp.utcnow().normalize()
    start_date_pairs = end_date_pairs - pd.Timedelta(days=30)

    # Download trusted pair volume for BTC/USDT in 1h frequency over last 30 days
    # We'll use the pair=btc-usdt, unified so it's comparable
    pair_trusted_vol = client.get_pair_metrics(
        pairs=['btc-usdt'],
        metrics=[trusted_metric],
        start_time=start_date_pairs.strftime('%Y-%m-%d'),
        end_time=end_date_pairs.strftime('%Y-%m-%d'),
        frequency='1h'
    ).to_dataframe()


    return end_date_pairs, pair_trusted_vol, start_date_pairs


@app.cell
def _(client, end_date_pairs, go, pair_trusted_vol, pd, start_date_pairs):
    mexc_candles_30d = client.get_market_candles(
        markets='mexc-btc-usdt-spot',
        frequency='1h',
        start_time=start_date_pairs,
        end_time=end_date_pairs,
    ).parallel().to_dataframe()

    mexc_candles_30d = mexc_candles_30d.set_index('time')

    # Merge with trusted pair for direct comparison
    _df_compare_candles = pd.DataFrame({'time': pair_trusted_vol['time']})
    _df_compare_candles = _df_compare_candles.merge(
        pair_trusted_vol[['time', 'volume_trusted_spot_usd_1h']].rename(
            columns={'volume_trusted_spot_usd_1h': 'trusted_pair_volume'}
        ),
        on='time', how='left'
    ).merge(
        mexc_candles_30d.rename(columns={'candle_usd_volume': 'mexc_candle_volume'}),
        on='time', how='left'
    )

    # Plot
    fig_compare_candles = go.Figure()
    fig_compare_candles.add_trace(go.Scatter(
        x=_df_compare_candles['time'], y=_df_compare_candles['trusted_pair_volume'],
        mode='lines', name='BTC/USDT Trusted Pair Volume', line=dict(color='blue')
    ))
    fig_compare_candles.add_trace(go.Scatter(
        x=_df_compare_candles['time'], y=_df_compare_candles['mexc_candle_volume'],
        mode='lines', name='MEXC BTC-USDT Candle Volume', line=dict(color='orange')
    ))
    fig_compare_candles.update_layout(
        title="BTC/USDT Trusted Pair Volume vs MEXC BTC-USDT Candle Volume (Hourly, 30d, Candle-based)",
        xaxis_title="",
        yaxis_title="Volume (USD)",
        legend_title="Volume Source"
    )

    fig_compare_candles

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
    r"""
    ## 📋 Crypto Trading Volume Cheat Sheet

    | Metric Name                              | What It Measures                                          | Source & Notes                                                                                        | Use Cases                              |
    |------------------------------------------|-----------------------------------------------------------|------------------------------------------------------------------------------------------------------|----------------------------------------|
    | **volume_reported_spot_usd_1h**          | Reported spot trading volume (USD, 1hr)                   | Exchange self-reported (may include wash trade, non-standard lots, potential inflation)              | Market share, broad activity trends     |
    | **candle_usd_volume**                    | Trade volume as realized in market (USD, per candle)      | Aggregates actual executed trades within the specified market/candle (fewer fakes than reported)      | Market structure, price discovery      |
    | **volume_trusted_spot_usd_1h**           | "Trusted" spot volume (USD, 1hr) across reliable venues   | Coin Metrics curation: Sums volume only from vetted, reputable, liquid exchanges                     | Market sizing, benchmark comparisons   |
    | **Pair Metrics**                         | Volume for a specific trading pair across all venues      | Avoids double counting, includes only matched, like-for-like pairs (e.g. BTC/USDT)                   | Price & liquidity index construction   |

    **Key Points:**

    - **Reported Volume** may be overstated (fake/wash trading) depending on the exchange.
    - **Candle Volume** represents actual trades observed on single markets: usually more reliable, but may still include noise.
    - **Trusted Volume** (by Coin Metrics) is curated to exclude dubious/outlier exchanges; best for cross-market comparisons.
    - **Pair Metrics** aggregate all trading venues for a specific pair—used for "true" pair liquidity, but may use only trusted sources in some definitions.

    **Best Practices:**
    - Use **Trusted Volume** for size/benchmarking and comparisons.
    - Use **Candle Volume** to analyze individual markets/exchanges.
    - Use **Reported Volume** only for exchange-declared statistics—cross-check with other sources!
    - Use **Pair Metrics** for the deepest liquidity view of a specific asset pair.

    ---
    *Tip: Large discrepancies between reported and trusted/candle volumes may indicate wash trading or unreliable reporting by that exchange or market.*
    """)

    return


@app.cell
def _(client, f):
    im = ['volume_reported_spot_usd_1h', 'volume_trusted_spot_usd_1h']

    a = {}
    b = {}
    c = {}

    for im_ in im:
        print(im_)
        a[im_] = client.catalog_asset_metrics_v2(metrics=im_).to_dataframe()
        try:
            b[im_] = client.catalog_exchange_asset_metrics_v2(metrics=im_).to_dataframe()
        except:
            pass
        c[im_] = client.catalog_pair_metrics_v2(metrics=im_).to_dataframe()
        try:
            f[im_] = client.catalog_market_metrics_v2(metrics=im_).to_dataframe()
        except:
            pass
    
    d = client.catalog_pair_candles_v2().to_dataframe()
    e = client.catalog_market_candles_v2(market_type='spot').to_dataframe()


    return a, b, c, d, e


@app.cell
def _(a, b, c, d, e):
    a, b, c, d.loc[d.frequency=='1d'], e.loc[e.frequency=='1d']
    return


@app.cell
def _(a, b, c, d, e, pd):
    # Collect row counts for each DataFrame

    row_counts = pd.DataFrame(
        {
            'reported spot vol': [
                a['volume_reported_spot_usd_1h'].shape[0],
                b['volume_reported_spot_usd_1h'].shape[0],
                c['volume_reported_spot_usd_1h'].shape[0],
                0
            ],
            'trusted spot vol': [
                a['volume_trusted_spot_usd_1h'].shape[0],
                0,  # no trusted spot at exchange-asset level
                c['volume_trusted_spot_usd_1h'].shape[0],
                0,  # no market trusted vol
            ],
            'candle vol': [
                0,  # no asset candle aggregation
                0,  # no exchange-asset candle aggregation
                d.loc[d.frequency=='1d'].shape[0],  # pair candles
                e.loc[e.frequency=='1d'].shape[0],  # market candles
            ]
        },
        index=['asset', 'exchange-asset', 'pair', 'market']
    )

    row_counts

    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
