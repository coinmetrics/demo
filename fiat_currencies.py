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

    client = CoinMetricsClient(os.getenv('CM_API_KEY'))
    return (
        CoinMetricsClient,
        client,
        datetime,
        mo,
        os,
        pd,
        plt,
        time,
        timedelta,
    )


@app.cell
def _(pd):
    fiat_url = "https://datahub.io/core/currency-codes/_r/-/data/codes-all.csv"
    fiat_df = pd.read_csv(fiat_url)
    # Convert to string before applying lower, to handle non-string types/Nans
    fiat_df['asset'] = fiat_df['AlphabeticCode'].apply(lambda x: str(x).lower() if pd.notnull(x) else x)
    fiat_df

    return fiat_df, fiat_url


@app.cell
def _(fiat_df):
    c = fiat_df.groupby(['Currency', 'asset']).agg({'Entity':'first'}).reset_index()
    c
    return (c,)


@app.cell
def _(client):
    assets = client.catalog_asset_metrics_v2(metrics='ReferenceRateUSD').to_dataframe()

    return (assets,)


@app.cell
def _(assets):
    assets_consolidated = assets.groupby('asset').agg({'metric':'count', 'max_time':'max', 'min_time':'min'})
    return (assets_consolidated,)


@app.cell
def _(assets_consolidated, c):
    assets_consolidated.merge(c, on='asset')
    return


@app.cell
def _():
    return


@app.cell
def _(assets_consolidated, c):
    import matplotlib.dates as mdates

    # Filter to 27 fiat currencies that are in both DataFrames
    join_df = assets_consolidated.merge(c, on='asset')
    df_27 = join_df.sort_values('min_time')

    #df_27['min_time'] = pd.to_datetime(df_27['min_time'])
    #df_27['max_time'] = pd.to_datetime(df_27['max_time'])


    return df_27, join_df, mdates


@app.cell
def _(df_27):
    df_27
    return


@app.cell
def _(datetime, df_27, pd, timedelta):
    from matplotlib.colors import ListedColormap

    # Find the global minimum time across all currencies
    timeline_start = df_27['min_time'].min()

    # Determine which currencies are 'obsolete' (max_time older than 30 days ago)
    # Ensure cutoff_date is timezone-aware (UTC) to match tz-aware datetimes if needed
    if df_27['max_time'].dt.tz is not None:
        _cutoff_date = pd.Timestamp(datetime.now(), tz='UTC') - timedelta(days=30)
    else:
        _cutoff_date = datetime.now() - timedelta(days=30)

    df_27['is_obsolete'] = df_27['max_time'] < _cutoff_date



    return ListedColormap, timeline_start


@app.cell
def _(df_27):
    df_27
    return


@app.cell
def _(datetime, df_27, mdates, pd, plt, timeline_start):
    # Fixing label/index misalignment by using enumerate over df_27.iterrows(),
    # so that y (and thus labels/y-ticks) are sequential and match each other.

    fig2_fixed, ax2_fixed = plt.subplots(figsize=(12, 8))
    colors = {True: "tomato", False: "skyblue"}
    labels_fixed = []

    # Use enumerate to get sequential position for y/labels
    for y, (_, row) in enumerate(df_27.iterrows()):
        xstart = row['min_time']
        xend = row['max_time']
        color = colors[row['is_obsolete']]
        ax2_fixed.barh(
            y=y,
            width=(xend - xstart).days,
            left=xstart,
            height=0.75,
            color=color,
            edgecolor="black"
        )
        labels_fixed.append(row['Currency'])

    ax2_fixed.set_yticks(range(len(df_27)))
    ax2_fixed.set_yticklabels(labels_fixed)
    ax2_fixed.set_xlabel("Date")
    ax2_fixed.set_title("Fiat Currency Data: Mean Time to Max Time Coverage (Top 27)")

    # Start timeline at the earliest min_time
    ax2_fixed.set_xlim([timeline_start, datetime.now() if df_27['max_time'].dt.tz is None else pd.Timestamp(datetime.now(), tz='UTC')])

    # Custom legend for active and obsolete
    from matplotlib.patches import Patch
    legend_handles_fixed = [
        Patch(color=colors[False], label="Active (≤30 days old)"),
        Patch(color=colors[True], label="Obsolete (>30 days old)")
    ]
    ax2_fixed.legend(handles=legend_handles_fixed, loc='lower right')

    ax2_fixed.xaxis.set_major_locator(mdates.YearLocator())
    ax2_fixed.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))

    plt.tight_layout()
    plt.gca()

    return (
        Patch,
        ax2_fixed,
        color,
        colors,
        fig2_fixed,
        labels_fixed,
        legend_handles_fixed,
        row,
        xend,
        xstart,
        y,
    )


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
