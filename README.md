# yfinance tester package

## Option 1: Run with Docker (recommended)
Requirements: Docker Desktop installed.

```bash
docker build -t yfinance-app .
docker run --rm -p 5000:5000 yfinance-app
```

Open [http://localhost:5000](http://localhost:5000)

## Option 2: Run with Python
Requirements: Python 3.11+

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Open [http://127.0.0.1:5000](http://127.0.0.1:5000)

## Notes
- The app stores/imports data under `data/`.
- For Docker, logs and imported files are inside the container unless you mount a volume.
