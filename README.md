# Model Cluster Gateway

OpenAI-compatible single endpoint for a mixed cluster of local and hosted models.

## What it does

- Exposes one external API: `/v1/chat/completions`
- Exposes work-run API for project tasks: `/v1/work`
- Routes requests by keyword or explicit hint (`metadata.router_hint`)
- Supports provider pinning via `metadata.provider`:
  - provider family: `local|openai|gemini|groq|mistral|cerebras|openrouter|pollinations`
  - backend pin: `backend:<backend_name>` (for example `backend:doubleword`)
- Fails over across multiple backends in configured order
- Supports local LM Studio, Pollinations, and named OpenAI-compatible backends
- Maintains heartbeat status so routing can prefer healthy nodes
- Discovers loaded LM Studio models from each registry node (`GET /-/discovery`)

## Project layout

- `scripts/model_cluster_gateway.py` - gateway server
- `config/gateway_config.json` - routing and backend config
- shared LM Studio role registry - `../../config/local_model_registry.json` (referenced by `gateway_config.json`)

## Run

```bash
cd /home/joe/codex/active/autonomous-projects/projects/model-cluster-gateway
python3 scripts/model_cluster_gateway.py --host 0.0.0.0 --port 4010
```

Or with make helpers:

```bash
make run-lan PORT=4010
make status PORT=4010
make smoke-team PORT=4010
make dogfood-workflow PORT=4010
make benchmark-models
```

Shareable sanitized export (no key files, Pollinations-first MVP config):

```bash
make export-shareable
```

Bundle output:
- `artifacts/share/model-cluster-gateway-shareable/`
- `config/gateway_config.shareable.json` (Pollinations-only route order for no-key startup)
- `SHAREABLE_NOTES.md` (quick start + security notes)

Admin portal:

```bash
open http://127.0.0.1:4010/-/admin
```

## Key endpoints

- `GET /health` - basic health
- `GET /-/status` - cluster heartbeat status and backend health
- `GET /-/discovery` - force LM Studio discovery and return loaded models
- `GET /-/admin` - built-in admin portal (provider setup, chat preview, run monitor)
- `GET /-/admin/api/bootstrap` - admin bootstrap payload (providers/config/status/runs)
- `POST /-/admin/api/provider/upsert` - create/update named hosted provider backend
- `POST /-/admin/api/provider/delete` - remove named hosted provider backend
- `POST /-/admin/api/provider/set-enabled` - toggle hosted provider backend enabled state
- `POST /-/admin/api/provider/test` - provider connectivity smoke test
- `POST /-/admin/api/team/stage-roles` - set planner/solver/verifier/strict_checker/synthesizer role preferences
- `POST /-/admin/api/cognitive-routing` - update adaptive cognitive mode thresholds and per-mode budgets
- `POST /-/admin/api/local-model/upsert` - create/update LM Studio local model role mapping
- `POST /-/admin/api/local-model/delete` - remove LM Studio local model mapping
- `POST /-/admin/api/local-model/set-enabled` - toggle LM Studio local model mapping enabled state
- `GET /-/admin/api/logs` - query structured logs (`limit`, `event_type`, `level`, `request_id`, `run_id`)
- `GET /-/admin/api/logs/export` - export filtered logs as `jsonl|json|csv`
- `POST /-/admin/api/chat` - run a quick chat preview from the admin portal
- `GET /v1/models` - gateway model + discovered local loaded models
- `POST /v1/chat/completions` - OpenAI-compatible chat completions
- `POST /v1/team/chat/completions` - force team orchestration (planner -> solver -> verifier -> synthesis)
- `POST /v1/work` - create and execute a project run (`mode`: `research|coding|organize`)
- `GET /v1/work/{run_id}` - get run status and result
- `GET /v1/work/{run_id}/events` - get run trace events
- `GET /v1/work/{run_id}/events/stream` - stream run events as SSE
- `POST /v1/work/{run_id}/cancel` - cancel a running run
- `POST /v1/work/{run_id}/approve` - apply a pending organize plan with approval token
- `POST /-/reload` - reload config and reset cached status

## Example request

```bash
curl -sS http://127.0.0.1:4010/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "meta-cluster-1",
    "messages": [{"role": "user", "content": "Help me debug this Python stack trace"}],
    "metadata": {"router_hint": "coding"}
  }' | jq
```

```bash
curl -sS http://127.0.0.1:4010/v1/team/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "meta-cluster-1",
    "messages": [{"role": "user", "content": "Design a robust deployment plan and verify the risks"}]
  }' | jq
```

```bash
curl -sS http://127.0.0.1:4010/v1/work \
  -H 'content-type: application/json' \
  -d '{
    "mode": "research",
    "project_id": "model-cluster-gateway",
    "objective": "Identify immediate priorities and propose next coding steps",
    "constraints": ["keep API OpenAI-compatible", "favor measurable checks"],
    "metadata": {"verification_level": "strict"}
  }' | jq
```

Async run mode:

```bash
curl -sS http://127.0.0.1:4010/v1/work \
  -H 'content-type: application/json' \
  -d '{
    "mode": "research",
    "project_id": "model-cluster-gateway",
    "objective": "Prioritize next sprint goals",
    "metadata": {"async": true, "team_mode": "on"}
  }' | jq
```

Then poll `GET /v1/work/{run_id}` or stream events with
`GET /v1/work/{run_id}/events/stream?since=0&wait_seconds=20`.

Tool adapter payloads on `/v1/work`:
- `tools.files.paths`: list of workspace files to ingest into run context
- `tools.web.urls`: list of URLs to fetch snippets from
- `tools.web.search_queries`: list of web search queries (DuckDuckGo instant answers/topics)
- `tools.shell.command`: optional command (allowlisted; requires `metadata.allow_shell=true`)
  and, when enabled in config, privileged auth header (`Authorization: Bearer <key>` or `X-API-Key`)
- `metadata.cognitive_depth`: `low|standard|deep` (deep enables extra critique+revision reflection pass)
- `metadata.tools_autonomy`: `true|false` (default `true`); when enabled, models can request tools during reasoning
- `metadata.max_tool_calls`: optional per-request override for autonomous tool budget
  (also supports `metadata.tools_autonomy.max_tool_calls`)
- `metadata.cognitive_mode`: optional mode override
  (`fast_path|plan_solve|debate_verify|search_reflect`)
- `metadata.team_step_timeout_seconds`: optional per-request team stage timeout override

Autonomous tool protocol (used by `/v1/chat/completions`, `/v1/team/chat/completions`, and `/v1/work` internal team stages):
- Model emits: `TOOL_REQUEST: {"tool":"web_search|web_fetch|read_files|shell|memory_read", ...}`
- Gateway executes safe tool call and returns a `TOOL_RESULT` message back into the same model loop
- Loop budget defaults from `tools_autonomy.max_tool_calls` in config (default `4`)
- Responses now include `tool_loop` with:
  `enabled`, `max_tool_calls`, `tool_calls_used`, `tool_calls_remaining`,
  `loop_termination_reason` (`assistant_finalized|tool_budget_exhausted|tools_autonomy_disabled`)
- Team responses now include `team.cognitive` and `team.budget` metadata for
  selected mode, difficulty score, and applied stage budgets.
- Team stage prompts are now role- and mode-aware (`planner`, `solver`,
  `verifier`, `strict_checker`, `synthesizer`) to align behavior with
  `fast_path|plan_solve|debate_verify|search_reflect`.

Organize mode safe apply split:
- Plan-only: `mode="organize"` with `organize_plan` and `metadata.organize_apply=false`
- Apply: call `POST /v1/work/{run_id}/approve` with the `approval.token` returned by plan-only run
- Approval token TTL is controlled by `work_execution.approval_ttl_seconds`
- Approval tokens are signed (HMAC) and single-use
- `organize_plan` currently supports:
  - `mkdir`: list of directories
  - `moves`: list of `{ "src": "...", "dst": "..." }`
- All operations are workspace-scoped and rejected outside the repo root.
- `metadata.organize_apply=true` and `/approve` can be auth-gated through `work_execution.privileged_actions`

Privileged action auth (optional hardening):
- Config path: `work_execution.privileged_actions`
- Set `enabled=true` and configure keys (`api_keys_env` and/or `api_keys_file`)
- Default required actions: `shell`, `organize_apply`, `approve`

Run storage + events:
- Runs are persisted under `artifacts/runs/` (`runs.json`, `events.jsonl`)
- Project memory is persisted under `artifacts/memory/project_memory.json` and recalled per `project_id`
- Structured gateway events are persisted under `artifacts/logs/gateway_events.jsonl`
  (`http_request`, `http_response`, `run_created`, `run_event`, `run_updated`,
  `provider_runtime`, `tool_request`, `tool_result`, `http_request_body`,
  `http_response_body`, `model_call_input`, `model_call_output`, `tool_context_collected`)
- SSE stream supports `?since=<index>&wait_seconds=<n>` for incremental consumers
- Async runs are queue-limited by `work_execution.max_concurrency` in `config/gateway_config.json`
- Async retries are controlled by `work_execution.retry_count` and `work_execution.retry_backoff_seconds`
- Provider-specific retries can be set with `work_execution.provider_retry.<provider>`
- Provider circuit breaker uses `work_execution.circuit_breaker` (`fail_threshold`, `cooldown_seconds`)
- Run timeout budget is controlled by `work_execution.run_timeout_seconds` (default 300s)
- `/v1/work` result `trace.timeout` now includes stage timing telemetry (`elapsed_ms`, `run_timeout_seconds`, `timed_out_stage`, per-stage checkpoints)
- Logging controls are in top-level `logging` config:
  - `enabled`
  - `path`
  - `max_payload_chars`
  - `capture_request_body`
  - `capture_response_body`
  - `include_model_transcripts`
  - `redact_secrets`

Dataset export from logs:
- Build chat-style training JSONL (normal `messages` template) from structured logs:
  - `make export-chat-dataset`
- Script:
  - `scripts/export_chat_dataset_from_logs.py`
- Default output:
  - `artifacts/datasets/chat_training_from_logs.jsonl`
- Note:
  - Restart the gateway after logging config/code updates so `http_request_body` and
    `http_response_body` events are present; otherwise export may produce zero rows.

Cognitive architecture on `/v1/work`:
- Recalls prior `facts`, `decisions`, `open_questions`, and `next_actions` by `project_id`
- Injects memory into the run prompt and requests explicit memory update sections
- Stores returned `memory_updates`, `open_questions`, and `next_actions` back to project memory
- Uses section-aware extraction for `Next steps`, `Memory updates`, and `Open questions` to reduce noisy memory writes
- Supports depth control with `metadata.cognitive_depth`:
  - `low`: speed-focused, defaults team mode off
  - `standard`: balanced default behavior
  - `deep`: adds reflection pass (critique then revision) before final answer

Citation evidence:
- Web fetch + web search tool outputs are scored against objective keyword overlap
- Top citations are attached under `result.evidence[].citations`

Benchmarking:
- Run rolling model matrix benchmark:
  - `make benchmark-models`
- Artifacts:
  - `artifacts/benchmarks/latest_matrix.json`
  - `artifacts/benchmarks/matrix_history.jsonl`
- Summary now includes weighted scoring and regression alerts:
  - `summary.weights` for normalized score weights
  - `summary.leader` for current top model
  - `summary.alerts` when score/pass-rate/latency regress vs prior snapshot
  - `rows[].avg_extraction_quality_score` and `rows[].avg_memory_update_noise_ratio`
    from a structured `work_memory_extract` benchmark prompt
- Default benchmark set:
  - OpenAI: `gpt-5-chat-latest`
  - OpenRouter: `openai/gpt-5-chat`
  - Local LM Studio: `qwen/qwen3-coder-next`

## Notes

- Pollinations is configured by default and can be used without API setup in the shareable MVP config.
- If `OPENAI_API_KEY` is set, the sample `openai_primary` backend can be used as an additional failover.
- `openai_primary` now defaults to `gpt-5-chat-latest`.
- If `GEMINI_API_KEY` is set (or `gemini_api_key.txt` exists at `/home/joe/codex/gemini_api_key.txt`), `gemini_primary` is available through the OpenAI-compatible Gemini endpoint with model `gemini-2.5-flash`.
- If `GROQ_API_KEY` is set (or `groq_api_key.txt` exists at `/home/joe/codex/groq_api_key.txt`), `groq_primary` is available with model `moonshotai/kimi-k2-instruct-0905`.
- If `MISTRAL_API_KEY` is set (or `mistral_api_key.txt` exists at `/home/joe/codex/mistral_api_key.txt`), `mistral_primary` is available with model `devstral-2512`.
- If `CEREBRAS_API_KEY` is set (or `cerebras_api_key.txt` exists at `/home/joe/codex/cerebras_api_key.txt`), `cerebras_primary` is available with model `gpt-oss-120b`.
- If `OPENROUTER_API_KEY` is set (or `openrouter_api_key.txt` exists at `/home/joe/codex/openrouter_api_key.txt`), `openrouter_primary` is available with model `openai/gpt-5-chat`.
- Free/entry tiers (check current pricing/terms before publishing; these can change):
  - Gemini: https://ai.google.dev/gemini-api/docs/pricing
  - Groq: https://console.groq.com/docs/rate-limits
  - Mistral: https://docs.mistral.ai/getting-started/quickstart/
  - Cerebras: https://www.cerebras.ai/inference
  - OpenRouter: https://openrouter.ai/settings/credits
- Heartbeat marks local nodes healthy only when the configured model is currently loaded on the node.
- On `/v1/chat/completions`, team mode is auto-decided unless you set `metadata.team_mode`:
  - `on` forces team orchestration
  - `off` forces single-model mode
  - `auto` (default) uses heuristic decision
- You can force provider family with `metadata.provider`:
  - `local` uses LM Studio backends only
  - `openai` uses OpenAI-named backend entries
  - `gemini` uses Gemini-named backend entries
  - `groq` uses Groq backend entries
  - `mistral` uses Mistral backend entries
  - `cerebras` uses Cerebras backend entries
  - `openrouter` uses OpenRouter backend entries
  - `pollinations` uses Pollinations backend only
- Verification depth can be set with `metadata.verification_level`:
  - `light`: planner + solver + synthesizer (skips verifier step)
  - `standard`: planner + solver + verifier + synthesizer
  - `strict`: forces team mode and adds a second verification pass (`strict_checker`)
- Team behavior is configurable in `team_orchestration` (`default_team_size`, `default_verification_level`, `step_timeout_seconds`, `team_keywords`).
- Cognitive mode selection and dynamic budgets are configurable in `cognitive_routing`:
  - `enabled`
  - `difficulty_thresholds` (`plan_solve`, `debate_verify`, `search_reflect`)
  - `budget_profiles` per mode (`team_size`, `verification_level`,
    `max_tool_calls`, `step_timeout_seconds`)
- Hosted providers can be enabled/disabled with backend-level `enabled` (defaults to `true`).
- Team stage assignments can be manually overridden with `team_orchestration.stage_roles`:
  - keys: `planner`, `solver`, `verifier`, `strict_checker`, `synthesizer`
  - values: `auto`, `provider:<family>`, `backend:<backend_name>`, or `local_role:<role>`

## Provider pin example

```bash
curl -sS http://127.0.0.1:4010/v1/chat/completions \
  -H 'content-type: application/json' \
  -d '{
    "model": "meta-cluster-1",
    "messages": [{"role": "user", "content": "Summarize this code review risk list"}],
    "metadata": {"router_hint": "reasoning", "provider": "openai"}
  }' | jq
```

## Admin portal notes

- Provider setup writes changes to `config/gateway_config.json` and reloads gateway state.
- Local model setup writes changes to `../../config/local_model_registry.json`.
- Local model mappings support explicit `enabled` on/off state in admin; disabled mappings are excluded from routing.
- If you provide `api_key_value` in the portal, it is written to `api_key_file` with restrictive permissions when possible.
- Run monitor reads existing `/v1/work/{run_id}` + `/events` data; no separate worker is required.
- Admin portal basic auth is controlled by `admin_portal` config:
  - `require_basic_auth` (enabled by default in this project config)
  - `basic_auth_user_env` / `basic_auth_pass_env` (default: `GATEWAY_ADMIN_USER` / `GATEWAY_ADMIN_PASS`)
- Intended flow:
  - manage providers/keys/models in `/-/admin`
  - use Open WebUI against this gateway for day-to-day chat UX

## Periodic dogfood testing

Use a real project-style prompt cycle to validate that orchestration and provider pins still work:

```bash
make dogfood-workflow PORT=4010
```

This generates a report at `artifacts/dogfood/latest_report.json` with:
- `/v1/work` project-cycle pass/fail for a project-next-steps task
- provider pin checks (`groq,mistral,cerebras,openrouter`) with a minimum success threshold
- `/v1/work` extraction metrics:
  - `work_extraction_quality_score`
  - `work_memory_update_noise_ratio`
  - per-scenario `extraction_metrics` and `extraction_ok`

Scheduled CI is included at `.github/workflows/model-gateway-dogfood.yml` (every 6 hours + manual dispatch).
