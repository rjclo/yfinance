# Twelve Data Stock Chart App

This app loads market data from Twelve Data and renders interactive charts with MA/RSI overlays.

## Prerequisites
- Python 3.11+ (or Docker)
- A Twelve Data API key

## API Key Setup
Set your key in the same shell where you run the app.

```bash
export TWELVE_DATA_API_KEY='your_twelve_data_key'
```

## Optional: AI Chart Explanation Key
The hint/lightbulb explain feature requires an OpenAI API key.

```bash
export OPENAI_API_KEY='your_openai_api_key'
```

Optional model override (default is `gpt-4.1-mini`):

```bash
export OPENAI_CHART_MODEL='gpt-4.1-mini'
```

## Option 1: Run With Python
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export TWELVE_DATA_API_KEY='your_twelve_data_key'
export OPENAI_API_KEY='your_openai_api_key'  # optional, for AI explain
python app.py
```

Open [http://127.0.0.1:5000](http://127.0.0.1:5000)

## Option 2: Run With Docker
```bash
docker build -t yfinance-app .
docker run --rm -p 5000:5000 \
  -e TWELVE_DATA_API_KEY='your_twelve_data_key' \
  -e OPENAI_API_KEY='your_openai_api_key' \
  yfinance-app
```

Open [http://localhost:5000](http://localhost:5000)

## How The Left And Right Tabs Work

### Left side: Ticker Categories
- Left tabs are your watchlist categories.
- Each tab contains ticker rows with selection checkboxes.
- Up to 5 selected tickers are loaded onto the chart.
- Left side is where you organize symbols you want to monitor.

### Right side: Imported Holdings Tabs
- Right tabs are created from imported CSV portfolios.
- Each right tab shows imported holdings for that portfolio.
- You can select holdings and push symbols into left categories for charting.
- Right side is for account/portfolio management; left side is for chart watchlists.

### CSV Import Coverage
- Fidelity and Schwab CSV formats are currently supported/verified.
- Other brokerage CSV formats may import partially or fail and are not yet verified.

## Data And Logs
- App data is stored under `data/`
- Server/client logs are under `data/logs/`
- Category and imported portfolio state is persisted under `data/categories/` and `data/imported_portfolios/`
