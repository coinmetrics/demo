import marimo

__generated_with = "0.12.9"
app = marimo.App(width="medium")


@app.cell
def _():
    from coinmetrics.api_client import CoinMetricsClient
    import os
    import marimo as mo
    from datetime import datetime, timedelta, time
    import matplotlib.pyplot as plt
    import pandas as pd
    import plotly.express as px

    client = CoinMetricsClient(os.getenv('CM_API_KEY'))

    return (
        CoinMetricsClient,
        client,
        datetime,
        mo,
        os,
        pd,
        plt,
        px,
        time,
        timedelta,
    )


@app.cell
def _():
    ## Base, Quote and Margin
    return


@app.cell
def _():
    my_metric = 'volume_reported_option_market_value_usd_1d'
    start_time='2025-01-01'
    return my_metric, start_time


@app.cell
def _(client, my_metric):
    cat = client.catalog_asset_metrics_v2(metrics=my_metric).to_dataframe().sort_values('max_time').set_index('asset')
    cat
    return (cat,)


@app.cell
def _(cat):
    usd_assets = ['usd', 'usdt', 'usdc']
    assets = set(cat.index) - set(usd_assets)
    assets
    return assets, usd_assets


@app.cell
def _(assets, client, my_metric, start_time):
    daily_vol = client.get_asset_metrics(assets=list(assets), metrics=my_metric, start_time=start_time).to_dataframe()
    daily_vol
    return (daily_vol,)


@app.cell
def _(daily_vol, px):
    fig_log = px.line(daily_vol, x='time', y='volume_reported_option_market_value_usd_1d', color='asset',
                       title='Daily Reported Option Market Volume (USD) -- All Markets (Log Scale)',
                       labels={'volume_reported_option_market_value_usd_1d': 'Volume (USD)', 'time': 'Date'},
                       color_discrete_sequence=px.colors.qualitative.Set1)
    fig_log.update_xaxes(title_text='Date')
    fig_log.update_yaxes(title_text='Volume (USD)', type='log')
    fig_log.update_layout(legend_title_text='Assets')
    fig_log

    return (fig_log,)


@app.cell
def _(daily_vol, px):
    fig_bar = px.bar(daily_vol, x='time', y='volume_reported_option_market_value_usd_1d', color='asset',
                      title='Daily Reported Option Market Volume (USD) - All Markets',
                      labels={'volume_reported_option_market_value_usd_1d': 'Volume (USD)', 'time': 'Date'},
                      color_discrete_sequence=px.colors.qualitative.Set1, 
                      barmode='stack')
    fig_bar.update_xaxes(title_text='Date')
    fig_bar.update_yaxes(title_text='Volume (USD)')
    fig_bar.update_layout(legend_title_text='Assets')
    fig_bar

    return (fig_bar,)


@app.cell
def _(client, my_metric, start_time, usd_assets):
    daily_vol_usd = client.get_asset_metrics(
        assets=list(usd_assets), 
        metrics=my_metric, 
        start_time=start_time
    ).to_dataframe()
    return (daily_vol_usd,)


@app.cell
def _(mo):
    mo.md(
        r"""
        Each underlying is always quoted against one of the USD variants, i.e., USD, USDC, or USDT. Therefore, if plotted against the USD variant, the bar chart looks almost the same. The total heights of the bars are the same. 
        USD dominates the picture because Deribit options are quoted in USD.
        """
    )
    return


@app.cell
def _(daily_vol_usd, px):
    def _():
        fig_bar = px.bar(daily_vol_usd, x='time', y='volume_reported_option_market_value_usd_1d', color='asset',
                          title='Daily Reported Option Market Volume (USD) - USD Variants',
                          labels={'volume_reported_option_market_value_usd_1d': 'Volume (USD)', 'time': 'Date'},
                          color_discrete_sequence=px.colors.qualitative.Set1, 
                          #text='volume_reported_option_market_value_usd_1d', 
                          barmode='stack')
        fig_bar.update_xaxes(title_text='Date')
        fig_bar.update_yaxes(title_text='Volume (USD)')
        fig_bar.update_layout(legend_title_text='Assets')
        return fig_bar


    _()
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ## Using Margin Currency as a Dimension: USDT, USDC, and "Coin"
        """
    )
    return


@app.cell
def _(client):
    margin_currency_metrics = ['volume_reported_option_tether_margined_market_value_usd_1d']

    margin_currency_metrics_cat = client.catalog_asset_metrics_v2(metrics=margin_currency_metrics).to_dataframe()
    return margin_currency_metrics, margin_currency_metrics_cat


@app.cell
def _(
    client,
    margin_currency_metrics,
    margin_currency_metrics_cat,
    start_time,
):
    daily_vol_usdt = client.get_asset_metrics(
        assets=list(set(margin_currency_metrics_cat.asset)-{'usdt'}), 
        metrics=margin_currency_metrics, 
        start_time=start_time
    ).to_dataframe()
    return (daily_vol_usdt,)


@app.cell
def _(daily_vol_usdt, px):
    def _():
        fig_bar = px.bar(daily_vol_usdt, x='time', y='volume_reported_option_tether_margined_market_value_usd_1d', color='asset',
                          title='Daily Reported Option Market Volume (USD) - Only Tether Margined',
                          labels={'volume_reported_option_tether_margined_market_value_usd_1d': 'Volume (USD)', 'time': 'Date'},
                          color_discrete_sequence=px.colors.qualitative.Set1, 
                          barmode='stack')
        fig_bar.update_xaxes(title_text='Date')
        fig_bar.update_yaxes(title_text='Volume (USD)')
        fig_bar.update_layout(legend_title_text='Assets')
        return fig_bar


    _()
    return


@app.cell
def _(mo):
    mo.md(r"""## Major interesting point above: Doge option volume.""")
    return


@app.cell
def _(client):
    margin_currency_metrics_usdc = ['volume_reported_option_usdc_margined_market_value_usd_1d']

    cat_usdc = client.catalog_asset_metrics_v2(metrics=margin_currency_metrics_usdc).to_dataframe()
    cat_usdc
    return cat_usdc, margin_currency_metrics_usdc


@app.cell
def _(cat_usdc, client, start_time):
    daily_vol_usdc = client.get_asset_metrics(
        assets=list(set(cat_usdc.asset) - {'usdc'}), 
        metrics='volume_reported_option_usdc_margined_market_value_usd_1d', 
        start_time=start_time
    ).to_dataframe()
    daily_vol_usdc
    return (daily_vol_usdc,)


@app.cell
def _(daily_vol_usdc, px):
    def _():
        fig_bar = px.bar(daily_vol_usdc, x='time', y='volume_reported_option_usdc_margined_market_value_usd_1d', color='asset',
                          title='Daily Reported Option Market Volume (USDC margined) - "Almost immaterial!"',
                          labels={'volume_reported_option_usdc_margined_market_value_usd_1d': 'Volume (USD)', 'time': 'Date'},
                          color_discrete_sequence=px.colors.qualitative.Set1, 
                          #text='volume_reported_option_market_value_usd_1d', 
                          barmode='stack')
        fig_bar.update_xaxes(title_text='Date')
        fig_bar.update_yaxes(title_text='Volume (USD)')
        fig_bar.update_layout(legend_title_text='Assets')
        return fig_bar


    _()
    return


@app.cell
def _(client):
    client.catalog_asset_metrics_v2(
        metrics='volume_reported_option_coin_margined_market_value_usd_1d', 
    ).to_dataframe()
    return


@app.cell
def _(client, start_time):
    daily_vol_coin_margined = client.get_asset_metrics(
        assets=['btc', 'eth', ], # brevity: only btc and eth are coin-margined
        metrics='volume_reported_option_coin_margined_market_value_usd_1d', 
        start_time=start_time
    ).to_dataframe()
    daily_vol_coin_margined
    return (daily_vol_coin_margined,)


@app.cell
def _(daily_vol_coin_margined, px):
    def _():
        fig_bar = px.bar(daily_vol_coin_margined, x='time', y='volume_reported_option_coin_margined_market_value_usd_1d', color='asset',
                          title='Daily Reported Option Market Volume (USD) Margin BTC or ETH',
                          labels={'volume_reported_option_coin_margined_market_value_usd_1d': 'Volume (USD)', 'time': 'Date'},
                          color_discrete_sequence=px.colors.qualitative.Set1, 
                          #text='volume_reported_option_market_value_usd_1d', 
                          barmode='stack')
        fig_bar.update_xaxes(title_text='Date')
        fig_bar.update_yaxes(title_text='Volume (USD)')
        fig_bar.update_layout(legend_title_text='Assets')
        return fig_bar


    _()
    return


@app.cell
def _(mo):
    mo.md(r"""We can conclude that the vast majority of the volume is coming from coin-margined option contracts, specifically BTC and ETH. Traders use BTC as margin collateral for trading BTC and ditto for ETH.""")
    return


@app.cell
def _(daily_vol, daily_vol_coin_margined, pd, px):
    daily_vol_grouped = daily_vol.loc[daily_vol.asset.isin(['btc', 'eth'])].groupby('time')['volume_reported_option_market_value_usd_1d'].sum().reset_index()
    daily_vol_coin_margined_grouped = daily_vol_coin_margined.groupby('time')['volume_reported_option_coin_margined_market_value_usd_1d'].sum().reset_index()

    # Merging datasets on 'time'
    merged_vol = pd.merge(daily_vol_grouped, daily_vol_coin_margined_grouped, on='time', suffixes=('_total', '_coin_margined'))

    # Calculating percentage of coin margined volume
    merged_vol['percentage_coin_margined'] = (merged_vol['volume_reported_option_coin_margined_market_value_usd_1d'] / merged_vol['volume_reported_option_market_value_usd_1d']) * 100

    # Plotting the results
    fig_percentage = px.line(merged_vol, x='time', y='percentage_coin_margined',
                              title='Percentage of Coin Margined Volume Relative to Total Volume (BTC/ETH)',
                              labels={'percentage_coin_margined': 'Percentage (%)', 'time': 'Date'},
                              color_discrete_sequence=['blue'])
    fig_percentage.update_xaxes(title_text='Date')
    fig_percentage.update_yaxes(title_text='Percentage of Coin Margined Volume (%)')
    fig_percentage.update_layout(legend_title_text='Coin Margined Volume')
    fig_percentage

    return (
        daily_vol_coin_margined_grouped,
        daily_vol_grouped,
        fig_percentage,
        merged_vol,
    )


@app.cell
def _(mo):
    mo.md(r"""## Exploring Coin-Margined Options (ETH and BTC)""")
    return


@app.cell
def _():
    exchange_metric = 'volume_reported_option_coin_margined_market_value_usd_1d'
    return (exchange_metric,)


@app.cell
def _(client, exchange_metric):
    ec = client.catalog_exchange_asset_metrics_v2(metrics=exchange_metric).to_dataframe()
    ec
    return (ec,)


@app.cell
def _(client, ec, exchange_metric, start_time):
    em = client.get_exchange_asset_metrics(
        metrics=exchange_metric,
        exchange_assets=list(set(ec.exchange_asset)- {'deribit-usd', 'okex-usd'}),
        start_time=start_time
    ).to_dataframe()
    return (em,)


@app.cell
def _(em, px):
    fig_stacked_bar = px.bar(em, x='time', y='volume_reported_option_coin_margined_market_value_usd_1d', 
                              color='exchange_asset',
                              title='Daily Reported Option Market Volume (USD) - Coin Margined by Exchange',
                              labels={'volume_reported_option_coin_margined_market_value_usd_1d': 'Volume (USD)', 'time': 'Date'},
                              color_discrete_sequence=px.colors.qualitative.Set1,
                              barmode='stack')
    fig_stacked_bar.update_xaxes(title_text='Date')
    fig_stacked_bar.update_yaxes(title_text='Volume (USD)')
    fig_stacked_bar.update_layout(legend_title_text='Exchange Assets')
    fig_stacked_bar

    return (fig_stacked_bar,)


@app.cell
def _(client, exchange_metric):
    ex = client.catalog_exchange_metrics_v2(metrics=exchange_metric).to_dataframe()
    ex
    return (ex,)


@app.cell
def _(client, ex, exchange_metric, start_time):
    em2 = client.get_exchange_metrics(
        metrics=exchange_metric,
        exchanges = list(ex.exchange),
        start_time=start_time
    ).to_dataframe()
    em2
    return (em2,)


@app.cell
def _(em2, px):
    fig_em2_stacked_bar = px.bar(em2, x='time', y='volume_reported_option_coin_margined_market_value_usd_1d', 
                                  color='exchange',
                                  title='Daily Reported Option Market Volume (USD) - Coin Margined by Exchange (EM2)',
                                  labels={'volume_reported_option_coin_margined_market_value_usd_1d': 'Volume (USD)', 'time': 'Date'},
                                  color_discrete_sequence=px.colors.qualitative.Set1,
                                  barmode='stack')
    fig_em2_stacked_bar.update_xaxes(title_text='Date')
    fig_em2_stacked_bar.update_yaxes(title_text='Volume (USD)')
    fig_em2_stacked_bar.update_layout(legend_title_text='Exchange')
    fig_em2_stacked_bar

    return (fig_em2_stacked_bar,)


@app.cell
def _(em2, px):
    # Calculate the market share of Deribit relative to the total volume
    em2['total_volume'] = em2.groupby('time')['volume_reported_option_coin_margined_market_value_usd_1d'].transform('sum')
    em2['deribit_volume'] = em2.loc[em2['exchange'] == 'deribit', 'volume_reported_option_coin_margined_market_value_usd_1d'].fillna(0)
    em2['relative_market_share'] = (em2['deribit_volume'] / em2['total_volume']) * 100

    # Creating the line chart for Deribit's relative market share
    fig_deribit_market_share = px.line(em2, x='time', y='relative_market_share',
                                         title='Relative Market Share of Deribit (coin-margined only)',
                                         labels={'relative_market_share': 'Market Share (%)', 'time': 'Date'},
                                         color_discrete_sequence=['orange'])
    fig_deribit_market_share.update_xaxes(title_text='Date')
    fig_deribit_market_share.update_yaxes(title_text='Relative Market Share (%)')
    fig_deribit_market_share.update_layout(legend_title_text='Deribit Market Share')
    fig_deribit_market_share

    return (fig_deribit_market_share,)


@app.cell
def _(mo):
    mo.md(r"""## Pairs""")
    return


@app.cell
def _(client):
    pc = client.catalog_pair_metrics_v2(metrics='volume_reported_option_market_value_usd_1d').to_dataframe()
    pc
    return (pc,)


@app.cell
def _(client, pc, start_time):
    pm = client.get_pair_metrics(
        pairs=list(set(pc.loc[pc.max_time>start_time, 'pair'])),
        metrics='volume_reported_option_market_value_usd_1d',
        start_time=start_time
    ).parallel().to_dataframe()
    pm
    return (pm,)


@app.cell
def _(pm, px):
    fig_pm_stacked_bar = px.bar(pm, x='time', y='volume_reported_option_market_value_usd_1d', 
                                 color='pair',
                                 title='Daily Reported Option Market Volume (USD) - by Pair',
                                 labels={'volume_reported_option_market_value_usd_1d': 'Volume (USD)', 'time': 'Date'},
                                 color_discrete_sequence=px.colors.qualitative.Set1,
                                 barmode='stack')
    fig_pm_stacked_bar.update_xaxes(title_text='Date')
    fig_pm_stacked_bar.update_yaxes(title_text='Volume (USD)')
    fig_pm_stacked_bar.update_layout(legend_title_text='Pairs')
    fig_pm_stacked_bar

    return (fig_pm_stacked_bar,)


@app.cell
def _(client):
    client.reference_data_markets(base='doge').to_dataframe().groupby(['type', 'exchange'], observed=True).size()
    return


@app.cell
def _(client, start_time):
    fv = client.get_asset_metrics(
        assets='doge',
        metrics='volume_reported_future_usd_1d',
        start_time=start_time
    ).to_dataframe()

    return (fv,)


@app.cell
def _(fv, px):
    fig_future_volume = px.bar(fv, x='time', y='volume_reported_future_usd_1d',
                                 title='Daily Reported Future Volume (USD) - Doge',
                                 labels={'volume_reported_future_usd_1d': 'Volume (USD)', 'time': 'Date'},
                                 color_discrete_sequence=px.colors.qualitative.Set1)
    fig_future_volume.update_xaxes(title_text='Date')
    fig_future_volume.update_yaxes(title_text='Volume (USD)')
    fig_future_volume.update_layout(legend_title_text='Doge Future Volume')
    fig_future_volume

    return (fig_future_volume,)


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
