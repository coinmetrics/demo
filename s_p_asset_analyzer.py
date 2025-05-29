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


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's obtain all reference data for all assets. """)
    return


@app.cell
def _(client):
    sec = client.security_master_assets().to_dataframe()
    sec
    return (sec,)


@app.cell
def _(client, sec):
    assets = client.reference_data_assets().to_dataframe().merge(sec)
    assets.rename(columns={'parent_asset': 'network'}, inplace=True)
    assets
    return (assets,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's see which assets have metrics and restrict our work on those assets that do.""")
    return


@app.cell
def _(client):
    assets_with_metrics_ = client.catalog_asset_metrics_v2().to_dataframe()
    assets_with_metrics__ = assets_with_metrics_.groupby('asset').agg({'max_time': max})
    assets_with_metrics__

    return assets_with_metrics_, assets_with_metrics__


@app.cell
def _(assets_with_metrics__, datetime, pd, timedelta):
    # Calculate 24 hours ago
    x_24h_ago = pd.Timestamp(datetime.utcnow() - timedelta(hours=72), tz="UTC")
    # Filter for assets with data within the last 24 hours
    assets_with_recent_metrics = assets_with_metrics__.loc[
        assets_with_metrics__['max_time'] >= x_24h_ago
    ]
    assets_with_recent_metrics

    return assets_with_recent_metrics, x_24h_ago


@app.cell
def _(assets, assets_with_recent_metrics):
    assets_with_metrics = assets.loc[assets.asset.isin(assets_with_recent_metrics.index)]
    assets_with_metrics
    return (assets_with_metrics,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's download the asset profiles and the network profiles. Since they are very nested, the `to_dataframe` method does not work very well. """)
    return


@app.cell
def _(client, pd):
    profiles = pd.DataFrame( client.get_asset_profiles().to_list())
    profiles
    return (profiles,)


@app.cell
def _(client, pd):
    networks = pd.DataFrame(client.get_network_profiles().to_list())
    networks
    return (networks,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's see how many parts each asset has by splitting on the underscore. """)
    return


@app.cell
def _(assets_with_metrics):
    assets_with_metrics.loc[:, 'underscore_count'] = assets_with_metrics['asset'].str.count('_')

    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's map the appropriate parts of the composite asset ticker to base, version and network. """)
    return


@app.cell
def _(assets_with_metrics, pd):
    def split_asset_improved(row):
        parts = row['asset'].split('_')
        underscore_count = row['underscore_count']
        # Helper to test if part is a number
        def is_number(value):
            try:
                float(value)
                return True
            except (ValueError, TypeError):
                return False
        if underscore_count == 2:
            # asset_main, version, network (unless version is not a number, then treat as network/bridge)
            if is_number(parts[1]):
                return pd.Series({'asset_main': parts[0], 'version': parts[1], 'network': parts[2], 'bridge': None})
            else:
                # interpret: asset_main, network, bridge
                return pd.Series({'asset_main': parts[0], 'version': None, 'network': parts[1], 'bridge': parts[2]})
        elif underscore_count == 1:
            # asset_main, network OR asset_main, bridge (if second part is not a number)
            if is_number(parts[1]):
                return pd.Series({'asset_main': parts[0], 'version': parts[1], 'network': None, 'bridge': None})
            else:
                return pd.Series({'asset_main': parts[0], 'version': None, 'network': parts[1], 'bridge': None})
        else:
            return pd.Series({'asset_main': row['asset'], 'version': None, 'network': None, 'bridge': None})

    # Demonstrate on assets_with_metrics DataFrame
    assets_split_v2 = assets_with_metrics.merge(assets_with_metrics.apply(split_asset_improved, axis=1), left_index=True, right_index=True, suffixes=['_secmaster', ''])
    assets_split_v2.loc[assets_split_v2.network.isnull(),'network'] = assets_split_v2.network_secmaster
    assets_split_v2

    return assets_split_v2, split_asset_improved


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's identify bridged assets by checking for `.e` (which means from Ethereum) or `.t` (which means from Terra). """)
    return


@app.cell
def _(assets_split_v2):
    assets_split_v2['bridged'] = assets_split_v2['asset_main'].str.endswith(('.e', '.t')) 
    assets_split_v2.loc[assets_split_v2.bridged,'asset_main'] = assets_split_v2['asset_main'].str.replace(r'\.(e|t)$', '', regex=True)
    assets_split_v2.loc[:, ['full_name', 'asset_main', 'bridged', 'version', 'network']]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let us see how many assets per network we find. As we expected, Ethereum is number one on the list, followed by Solana, Optimism, and Base. """)
    return


@app.cell
def _(assets_split_v2):
    assets_split_v2.groupby('network').size().sort_values(ascending=False)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Now we can merge the asset profile so that we get the token purpose. """)
    return


@app.cell
def _(assets_split_v2, networks, profiles):
    assets_split_v2.loc[
        (assets_split_v2.network.isin(list(networks.network) + ['op.eth', 'base.eth'])),
        :
    ].merge(profiles, left_on='asset_main', right_on='asset', suffixes=['_asset', '_network']).loc[:, ['full_name_asset', 'asset_asset', 'bridged', 'network', 'token_purpose', 'version']]
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""So far, we have 5 assets on Base and 5 on Optimism. """)
    return


@app.cell
def _(assets_split_v2):
    # Assets on Base
    assets_split_v2.loc[assets_split_v2.network=='base.eth']
    return


@app.cell
def _(assets_split_v2):
    # Assets on Optimism
    assets_split_v2.loc[assets_split_v2.network=='op.eth']
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""The native assets of the network need to be mapped to their network. Then we can merge the network profile to the assets on the network name. This also gives us access to the network type, which could be "roll-up" to show a layer 2. """)
    return


@app.cell
def _(assets_split_v2, networks):
    for n in networks.network:
        assets_split_v2.loc[assets_split_v2.asset==n, 'network'] = n

    return (n,)


@app.cell
def _(assets_split_v2, networks):
    assets_split_v2.merge(networks, on='network', how='left', suffixes=['_asset', '_network'])
    return


@app.cell
def _(mo):
    mo.md(
        r"""
        ### Peculiarities

        `eth_base` is on network `base`, which is called `base.eth` for all base tokens.

        `eth_op` is on network `op`, which is called `op.eth` for all op tokens.

        Optimism and Base do not have network profiles yet.
        """
    )
    return


@app.cell
def _(assets_split_v2):
    network_0 = assets_split_v2.loc[assets_split_v2.network.isnull()]
    network_0
    return (network_0,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""Let's see if we can use asset metrics to determine which of assets without an explicit network live on Ethereum. """)
    return


@app.cell
def _(client):
    cat = client.catalog_asset_metrics_v2().to_dataframe()

    return (cat,)


@app.cell
def _(cat, network_0):
    cat.loc[cat.asset.isin(network_0.asset)].groupby('metric').size()
    return


@app.cell
def _(cat, network_0):
    b = cat.loc[(cat.metric=='BlkCnt') & (cat.asset.isin(network_0.asset))]
    b
    return (b,)


@app.cell
def _(assets_split_v2, b):
    assets_split_v2.loc[assets_split_v2.asset.isin(b.asset), 'network'] =  assets_split_v2.loc[assets_split_v2.asset.isin(b.asset), 'asset']
    return


@app.cell
def _(assets_split_v2, b):
    assets_split_v2.loc[assets_split_v2.asset.isin(b.asset), :]
    return


@app.cell
def _(sec):
    sec.loc[(sec.parent_asset.notnull())]
    return


@app.cell
def _(profiles, sec):
    # Number of assets with matching description in both dataframes
    assets_desc_set = set(profiles['description'].dropna().unique())
    sec_desc_set = set(sec['description'].dropna().unique())
    matching_descriptions = assets_desc_set & sec_desc_set
    num_matching = len(matching_descriptions)

    # Assets existing in 'assets' but missing in 'sec'
    assets_only = assets_desc_set - sec_desc_set

    # Assets existing in 'sec' but missing in 'assets'
    sec_only = sec_desc_set - assets_desc_set

    result = {
        "num_matching_descriptions": num_matching,
        "assets_only_descriptions": assets_only,
        "sec_only_descriptions": sec_only
    }
    result

    return (
        assets_desc_set,
        assets_only,
        matching_descriptions,
        num_matching,
        result,
        sec_desc_set,
        sec_only,
    )


@app.cell
def _(profiles, sec):
    profiles_only_cols = set(profiles.columns) - set(sec.columns)
    sec_only_cols = set(sec.columns) - set(profiles.columns)

    comparison = {
        "columns_only_in_profiles": profiles_only_cols,
        "columns_only_in_sec": sec_only_cols
    }
    comparison

    return comparison, profiles_only_cols, sec_only_cols


@app.cell
def _(profiles, sec):
    set(sec.columns).intersection(set(profiles.columns))
    return


@app.cell
def _(client):
    markets = client.reference_data_markets(type='spot').to_dataframe()
    markets
    return (markets,)


@app.cell
def _(markets):
    markets.loc[markets.base=='eos', ['market', 'base','base_native']]
    return


@app.cell
def _(markets):
    markets.loc[markets.base_native=='A', ['market', 'base','base_native']]
    return


@app.cell
def _(markets):
    markets.loc[markets.base_native.notnull() & markets.base_native.str.contains('vault'), ['market', 'base','base_native']]
    return


@app.cell
def _(assets):
    assets.loc[assets.asset=='a']
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
