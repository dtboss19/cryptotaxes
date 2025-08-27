# Solana to TokenTax CSV Exporter

This tool fetches enriched Solana transactions via Helius and exports a CSV suitable for TokenTax-style import.

## Prerequisites
- Python 3.9+
- Helius API Key (`HELIUS_API_KEY` environment variable)

## Install
```bash
python -m venv .venv
. .venv/Scripts/Activate.ps1  # PowerShell on Windows
pip install -r requirements.txt
```

## Configure Wallets
Edit `wallets.json` and list all of your wallet addresses. Transfers between any of these will be flagged as self-transfers.

## Run
```bash
python helius_export.py --output output.csv --wallets wallets.json --limit 1000
```

Options:
- `--api-key` HELIUS API key (overrides `HELIUS_API_KEY` env var)
- `--wallets` Path to wallets JSON file
- `--output` Destination CSV path
- `--start` ISO timestamp (inclusive) e.g. 2023-01-01T00:00:00Z
- `--end` ISO timestamp (exclusive)
- `--limit` Max transactions per wallet to fetch (pagination handled automatically)

## Notes
- TYPE derivation is heuristic-based and uses Helius enriched categories and sources. Adjust in `derive_transaction_type` as needed.
- Cost basis requires pricing data; this version leaves `cost_basis_usd` empty for now.
