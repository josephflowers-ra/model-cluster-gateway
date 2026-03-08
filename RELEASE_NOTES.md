# Release Notes

## v0.1.0 - Shareable MVP

Date: 2026-03-08

Highlights:
- OpenAI-compatible gateway with `/v1/chat/completions`, `/v1/team/chat/completions`, and `/v1/work`.
- Built-in admin portal at `/-/admin` for provider setup and chat preview.
- Pollinations-first shareable config (`config/gateway_config.shareable.json`) for no-key startup.
- Team orchestration with stage tracing, verification levels, and cooldown-aware routing behavior.

Operational defaults:
- `run_gateway.sh` loads `.env` if present.
- If admin credentials are not set, local defaults are used:
  - `GATEWAY_ADMIN_USER=admin`
  - `GATEWAY_ADMIN_PASS=adminpass`

Upgrade notes:
- For production, set explicit admin credentials in env and rotate them.
- Enable additional providers by configuring API keys and routing in `config/gateway_config.json`.
