# Largest Banks ETL (Wikipedia → CSV + SQLite)

This project:
1) Extracts the "By market capitalization" table from Wikipedia (LIVE page)
2) Transforms market cap from USD to GBP/EUR/INR using an exchange rate CSV
3) Loads results to CSV and SQLite
4) Runs example SQL queries (London/Berlin/New Delhi)

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
