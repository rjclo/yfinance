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

## AI Chart Explanation

The lightbulb icon opens an AI Explain panel that analyzes the visible chart window. It supports **OpenAI** and **Google Gemini** as providers.

### Option A: Configure in the browser (recommended)
1. Click the lightbulb icon to open the AI Explain panel
2. Select a provider (OpenAI or Gemini) and model from the grouped dropdown
3. Enter your API key and click **Save Settings**
4. Click **Explain** to generate analysis

Settings are saved to `data/ai_settings.json` and persist across sessions.

### Option B: Configure via environment variables
Env vars override saved settings when set.

```bash
export OPENAI_API_KEY='your_openai_api_key'
# or
export GEMINI_API_KEY='your_gemini_api_key'
```

Optional model overrides:

```bash
export OPENAI_CHART_MODEL='gpt-4.1-mini'    # default
export GEMINI_CHART_MODEL='gemini-2.5-flash' # default
```

## Option 1: Run With Python
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
export TWELVE_DATA_API_KEY='your_twelve_data_key'
python app.py
```

Open [http://127.0.0.1:5000](http://127.0.0.1:5000)

## Option 2: Run With Docker
```bash
docker build -t yfinance-app .
docker run --rm -p 5000:5000 \
  -e TWELVE_DATA_API_KEY='your_twelve_data_key' \
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
- AI provider settings and API keys are stored in `data/ai_settings.json`
