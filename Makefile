.PHONY: run run-lan status discovery models reload smoke-chat smoke-team dogfood-workflow benchmark-models export-chat-dataset hardening-regression export-shareable

HOST ?= 127.0.0.1
PORT ?= 4010

run:
	python3 scripts/model_cluster_gateway.py --config config/gateway_config.json --host $(HOST) --port $(PORT)

run-lan:
	python3 scripts/model_cluster_gateway.py --config config/gateway_config.json --host 0.0.0.0 --port $(PORT)

status:
	curl -sS http://127.0.0.1:$(PORT)/-/status | jq

discovery:
	curl -sS http://127.0.0.1:$(PORT)/-/discovery | jq

models:
	curl -sS http://127.0.0.1:$(PORT)/v1/models | jq

reload:
	curl -sS -X POST http://127.0.0.1:$(PORT)/-/reload | jq

smoke-chat:
	curl -sS http://127.0.0.1:$(PORT)/v1/chat/completions \
		-H 'content-type: application/json' \
		-d '{"model":"meta-cluster-1","metadata":{"team_mode":"off"},"messages":[{"role":"user","content":"Say hello in one line."}]}' | jq

smoke-team:
	curl -sS http://127.0.0.1:$(PORT)/v1/team/chat/completions \
		-H 'content-type: application/json' \
		-d '{"model":"meta-cluster-1","metadata":{"team_mode":"on","team_size":3,"router_hint":"reasoning"},"messages":[{"role":"user","content":"Rank likely root causes for periodic 502s and propose quick mitigations with verification."}]}' | jq

dogfood-workflow:
	python3 scripts/workflow_dogfood.py \
		--base-url http://127.0.0.1:$(PORT) \
		--project model-cluster-gateway \
		--providers groq,mistral,cerebras,openrouter \
		--min-provider-passes 2 \
		--out artifacts/dogfood/latest_report.json

benchmark-models:
	python3 scripts/model_benchmark_matrix.py \
		--out-dir artifacts/benchmarks \
		--openai-model gpt-5-chat-latest \
		--openrouter-model openai/gpt-5-chat \
		--include-local \
		--local-model qwen/qwen3-coder-next \
		--local-base-url http://10.0.0.11:1234/v1

export-chat-dataset:
	python3 scripts/export_chat_dataset_from_logs.py \
		--log-file artifacts/logs/gateway_events.jsonl \
		--out artifacts/datasets/chat_training_from_logs.jsonl \
		--include-work

hardening-regression:
	python3 scripts/hardening_regression.py \
		--base-url http://$(HOST):$(PORT) \
		--live \
		--trials 3 \
		--out artifacts/dogfood/hardening_regression_latest.json

export-shareable:
	python3 scripts/export_shareable_bundle.py \
		--mvp-pollinations-only \
		--out-dir artifacts/share/model-cluster-gateway-shareable
