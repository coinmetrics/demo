# Crypto Market Analysis Tools

A collection of Jupyter notebooks and Python scripts for analyzing cryptocurrency markets, including options, futures, and liquidations.

## Contents

- **Jupyter Notebooks**
  - [Current_Supply_a_year_on.ipynb](./Current_Supply_a_year_on.ipynb) - Analysis of cryptocurrency supply changes over time
  - [DyDx_Demo.ipynb](./DyDx_Demo.ipynb) - Demonstration of dYdX trading platform data analysis
  - [Futures.ipynb](./Futures.ipynb) - Cryptocurrency futures market analysis
  - [Market_Liquidations.ipynb](./Market_Liquidations.ipynb) - Analysis of market liquidation events

- **Marimo Notebooks**
  - [option_volume_metrics.py](./option_volume_metrics.py) - Interactive analysis of cryptocurrency option volumes using Marimo

## Features

- Visualization of cryptocurrency option market volumes by asset
- Analysis of margin currencies (USDT, USDC, and coin-margined)
- Exchange market share comparison
- Trading pair volume analysis
- Futures market volume tracking
- Market liquidation analysis

## Requirements

- Python 3.x
- Required packages (see specific notebook/script for dependencies):
  - pandas
  - matplotlib
  - plotly
  - coinmetrics (for API access)
  - Marimo (for interactive notebooks)

## Marimo Integration

This repository includes [Marimo](https://marimo.io) notebooks for interactive data analysis and visualization. Marimo is a reactive Python notebook that combines the power of Jupyter with modern interactivity features.

Key benefits of Marimo:
- Reactive computation (cells automatically update when dependencies change)
- Interactive UI elements
- Beautiful visualizations
- Clean code organization

To learn more about Marimo and how to use it with these notebooks, visit the [Marimo documentation](https://docs.marimo.io/).

## Usage

1. Clone this repository
2. Install the required dependencies
3. Set up your API keys as environment variables (e.g., CM_API_KEY for CoinMetrics)
4. Run the notebooks or scripts

For Marimo scripts:
```bash
pip install marimo
marimo run option_volume_metrics.py
```

## License

MIT License