# Shareable Bundle Notes

This bundle was sanitized for external sharing:
- Removed file-based API key references from provider configs.
- Excluded runtime artifacts/logs and secret files.

## Quick Start
1. `python3 -m pip install -r requirements.txt`
2. `python3 scripts/model_cluster_gateway.py --config config/gateway_config.shareable.json --host 0.0.0.0 --port 4010`
3. Open `http://127.0.0.1:4010/-/admin`

## Pollinations MVP Mode
- `config/gateway_config.shareable.json` is set to Pollinations-only routing for no-key startup.
- To enable other providers, add keys and switch to `config/gateway_config.json` or edit the shareable config.

## Security
- Do not commit real API keys or local key files into this bundle.
- Prefer environment variables for keys when enabling providers.
