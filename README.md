# Twelve Data Stock Chart App

This app loads market data from Twelve Data and renders interactive charts with technical overlays, AI chart explanation, news, scanner, and backtest tools.

## Prerequisites
- Python 3.11+ (or Docker)
- A Twelve Data API key

## Config And API Keys
API keys and provider settings are managed from the app UI:

1. Start the app
2. Open the center-panel `...` menu
3. Click `Config`
4. Enter any keys/settings you want to use
5. Click `Save Config`

Supported config entries:
- `TWELVE_DATA_API_KEY`
- `Auto Refresh (5m)` toggle
- `Open API Usage` button
- `FINNHUB_API_KEY`
- `News Popup` toggle
- AI provider and model defaults
- AI API keys for supported providers

Saved config is written to:
- `.env`

Load precedence is:
1. process environment variables
2. `.env`

If the same key exists in both places, the environment variable wins and is written back to `.env`.

You can still pre-seed values with environment variables before starting the app if you prefer:

```bash
export TWELVE_DATA_API_KEY='your_twelve_data_key'
export FINNHUB_API_KEY='your_finnhub_api_key'
export OPENAI_API_KEY='your_openai_api_key'
export GEMINI_API_KEY='your_gemini_api_key'
export GROQ_API_KEY='your_groq_api_key'
export ANTHROPIC_API_KEY='your_anthropic_api_key'
```

## AI Chart Explanation
The lightbulb icon opens the AI chart guide / explain dialog.

Current behavior:
- Opening the lightbulb dialog automatically runs AI summary for the current visible chart window
- The dialog has two tabs:
  - `Current Read`
  - `Next Outlook`
- It uses:
  - visible chart range
  - trend/regime state
  - current indicators
  - candlestick patterns
  - recent news
  - local tomorrow-forecast model output
- The dialog also includes a `Follow-up Chat` area under the two tabs so you can ask additional questions without rerunning the full explanation immediately

AI provider/model/API key settings are configured from `Config`, not inside the lightbulb dialog.

## Finnhub News
Ticker news uses Finnhub.

- Configure `FINNHUB_API_KEY` in `Config`
- Configure `News Popup` in `Config` if you want ticker-row selection to open the news panel automatically
- News is also sent to the AI explain flow as context

## Auto Refresh
- Configure `Auto Refresh (5m)` in `Config`
- When enabled, the current chart refreshes every 5 minutes

## Workspace Views
The app now has two top-level tabs:

### Chart
- Shows the main chart workspace only
- This includes:
  - the center chart panel
  - chart controls
  - overlays
  - AI explain
  - news
  - scanner / backtest / watch list dialogs

### Portfolio
- Shows the two portfolio-management panels side by side
- Left panel:
  - watchlist categories
  - categorized ticker rows
  - `Save` button for holdings/layout state
- Right panel:
  - imported CSV portfolio tabs
  - imported holdings list

The selected top-level tab is remembered locally and restored the next time the app opens.

## Option 1: Run In A Browser With Python
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export TWELVE_DATA_API_KEY='your_twelve_data_key'
python app.py
```

Open [http://127.0.0.1:5000](http://127.0.0.1:5000)

## Option 2: Run As A Desktop App
This project now includes a native macOS wrapper around the Flask app using `pywebview`.

### From Terminal
```bash
cd /Users/rlo/Documents/yfinance
source .venv/bin/activate
pip install -r requirements.txt
python desktop_app.py
```

### Double-click Launcher
You can launch the desktop window directly from Finder:

- `/Users/rlo/Documents/yfinance/Launch Stock Chart.command`

### macOS App Bundle
A macOS app bundle is also included:

- `/Users/rlo/Documents/yfinance/Stock Chart App.app`

Notes:
- The `.app` bundle launches the project from its current path.
- It expects the project virtualenv to exist at:
  - `/Users/rlo/Documents/yfinance/.venv`
- It uses the same config, data, and model files as the browser version.

## Option 3: Run With Docker
```bash
docker build -t yfinance-app .
docker run --rm -p 5000:5000 \
  -e TWELVE_DATA_API_KEY='your_twelve_data_key' \
  yfinance-app
```

Open [http://localhost:5000](http://localhost:5000)

## Portfolio View Details

### Left Panel: Ticker Categories
- Left tabs are your watchlist categories.
- Each tab contains ticker rows with selection checkboxes.
- Up to 5 selected tickers are loaded onto the chart.
- Use the `Save` button in the upper-right of this panel to persist the current portfolio/layout state.

### Right Panel: Imported Holdings Tabs
- Right tabs are created from imported CSV portfolios.
- Each right tab shows imported holdings for that portfolio.
- You can select holdings and copy symbols into left categories for charting.
- The right panel is for account/portfolio management; the left panel is for chart watchlists.

### CSV Import Coverage
- Fidelity and Schwab CSV formats are currently supported/verified.
- Other brokerage CSV formats may import partially or fail and are not yet verified.

### Importing A New CSV
- Go to `Portfolio`
- In the right panel, click the `+` tab
- Choose the new CSV from Finder
- Enter a portfolio tab name when prompted

If you use the **same portfolio tab name** as an existing imported portfolio:
- the old CSV file is replaced
- the existing portfolio tab is updated instead of duplicated
- saved imported-holdings state is preserved where keys still match

This is the intended monthly refresh flow when you want to overwrite a previous CSV import with an updated one.

## Back Test And Scanner

### Back Test
- Open center-panel `...` -> `Back Test`
- If any tickers are checked in the active left category, it backtests those checked tickers
- Otherwise it backtests all tickers across all left-panel categories
- The backtest dialog supports:
  - `More Signals`
  - `Strongest Alerts`

### Scanner
- Open center-panel `...` -> `Scanner`
- Scanner runs the trained daily model against all left-panel tickers across all categories
- It only reports current live signals that meet the scanner threshold
- Scanner saves its trained model to disk and reuses it on later runs
- Use `Retrain Model` inside the scanner dialog to rebuild the scanner model from scratch

### Watch List
- Open center-panel `...` -> `Watch List`
- Watch List uses the same banner/confluence logic shown above the chart
- Same-day watchlist results are cached and reused
- `Force refresh from Twelve Data` bypasses that cache and rebuilds results with throttling

### API Usage
- Open center-panel `...` -> `Config`
- Under `Market Data`, click `Open API Usage`
- This shows upstream API usage broken down by context such as:
  - `chart`
  - `watchlist`
  - `scanner`
  - `backtest`
  - `ai_explain`

## Data And Logs
- App data is stored under `data/`
- Server/client logs are under `data/logs/`
- Category and imported portfolio state is persisted under `data/categories/` and `data/imported_portfolios/`
- Scanner model artifacts are stored under `data/models/`
- Config is persisted in `.env`
