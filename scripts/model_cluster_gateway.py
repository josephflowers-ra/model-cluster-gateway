#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import concurrent.futures
import hashlib
import hmac
import json
import os
import re
import shlex
import subprocess
import threading
import time
import uuid
from collections import Counter, deque
from datetime import datetime, timezone
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

import requests


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _abs_path(path_str: str) -> Path:
    p = Path(path_str).expanduser()
    if not p.is_absolute():
        p = project_root() / path_str
    return p.resolve()


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64url_decode(text: str) -> bytes:
    padded = text + ("=" * ((4 - len(text) % 4) % 4))
    return base64.urlsafe_b64decode(padded.encode("ascii"))


@dataclass
class Backend:
    name: str
    kind: str
    model: str
    base_url: str
    provider: str = ""
    route_model: str = ""
    api_key_env: str = ""
    api_key_file: str = ""
    extra_headers: Dict[str, str] = field(default_factory=dict)
    source_role: str = ""


class GatewayRouter:
    def __init__(self, config_path: Path) -> None:
        self.config_path = config_path
        self.config = self._load_json(config_path)
        self._ephemeral_approval_secret = uuid.uuid4().hex
        self._lock = threading.Lock()

    def _load_json(self, path: Path) -> Dict[str, Any]:
        raw = path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError(f"Config must be a JSON object: {path}")
        return data

    def reload(self) -> None:
        with self._lock:
            self.config = self._load_json(self.config_path)

    def config_snapshot(self) -> Dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self.config, ensure_ascii=False))

    def save_config(self, new_config: Dict[str, Any]) -> None:
        if not isinstance(new_config, dict):
            raise RuntimeError("Config must be a JSON object")
        if not isinstance(new_config.get("routing"), dict):
            raise RuntimeError("Config must include routing object")
        if not isinstance(new_config.get("backends"), dict):
            raise RuntimeError("Config must include backends object")
        tmp = self.config_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(new_config, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self.config_path)
        with self._lock:
            self.config = json.loads(json.dumps(new_config, ensure_ascii=False))

    def gateway_model(self) -> str:
        v = str(self.config.get("gateway_model", "meta-cluster-1")).strip()
        return v or "meta-cluster-1"

    def request_timeout_seconds(self) -> int:
        v = int(self.config.get("timeout_seconds", 90))
        return max(5, v)

    def heartbeat_interval_seconds(self) -> int:
        hb = self.config.get("heartbeat", {})
        if isinstance(hb, dict):
            return max(5, int(hb.get("interval_seconds", 20)))
        return 20

    def health_timeout_seconds(self) -> int:
        hb = self.config.get("heartbeat", {})
        if isinstance(hb, dict):
            return max(1, int(hb.get("timeout_seconds", 4)))
        return 4

    def prefer_healthy(self) -> bool:
        hb = self.config.get("heartbeat", {})
        if isinstance(hb, dict):
            return bool(hb.get("prefer_healthy", True))
        return True

    def logging_settings(self) -> Dict[str, Any]:
        raw = self.config.get("logging", {})
        if isinstance(raw, dict):
            return dict(raw)
        return {}

    def logging_enabled(self) -> bool:
        cfg = self.logging_settings()
        return bool(cfg.get("enabled", True))

    def logging_path(self) -> Path:
        cfg = self.logging_settings()
        path = str(cfg.get("path", "artifacts/logs/gateway_events.jsonl")).strip() or "artifacts/logs/gateway_events.jsonl"
        return _abs_path(path)

    def logging_max_payload_chars(self) -> int:
        cfg = self.logging_settings()
        raw = int(cfg.get("max_payload_chars", 4000))
        if raw <= 0:
            return 0
        return max(200, min(5000000, raw))

    def logging_capture_request_body(self) -> bool:
        cfg = self.logging_settings()
        return bool(cfg.get("capture_request_body", True))

    def logging_capture_response_body(self) -> bool:
        cfg = self.logging_settings()
        return bool(cfg.get("capture_response_body", True))

    def logging_redact_secrets(self) -> bool:
        cfg = self.logging_settings()
        return bool(cfg.get("redact_secrets", False))

    def logging_include_model_transcripts(self) -> bool:
        cfg = self.logging_settings()
        return bool(cfg.get("include_model_transcripts", True))

    def team_settings(self) -> Dict[str, Any]:
        team = self.config.get("team_orchestration", {})
        if isinstance(team, dict):
            return dict(team)
        return {}

    def cognitive_routing_settings(self) -> Dict[str, Any]:
        raw = self.config.get("cognitive_routing", {})
        if isinstance(raw, dict):
            return dict(raw)
        return {}

    def cognitive_routing_enabled(self) -> bool:
        cfg = self.cognitive_routing_settings()
        return bool(cfg.get("enabled", True))

    def cognitive_thresholds(self) -> Dict[str, float]:
        cfg = self.cognitive_routing_settings()
        raw = cfg.get("difficulty_thresholds", {})
        if not isinstance(raw, dict):
            raw = {}
        plan = float(raw.get("plan_solve", 0.35))
        debate = float(raw.get("debate_verify", 0.62))
        search = float(raw.get("search_reflect", 0.80))
        plan = max(0.05, min(0.95, plan))
        debate = max(plan, min(0.98, debate))
        search = max(debate, min(0.99, search))
        return {"plan_solve": plan, "debate_verify": debate, "search_reflect": search}

    def cognitive_budget_profiles(self) -> Dict[str, Dict[str, Any]]:
        defaults: Dict[str, Dict[str, Any]] = {
            "fast_path": {"team_size": 2, "verification_level": "light", "max_tool_calls": 2, "step_timeout_seconds": 25},
            "plan_solve": {"team_size": 3, "verification_level": "standard", "max_tool_calls": 2, "step_timeout_seconds": 40},
            "debate_verify": {"team_size": 4, "verification_level": "strict", "max_tool_calls": 3, "step_timeout_seconds": 55},
            "search_reflect": {"team_size": 4, "verification_level": "strict", "max_tool_calls": 4, "step_timeout_seconds": 65},
        }
        cfg = self.cognitive_routing_settings()
        raw_profiles = cfg.get("budget_profiles", {})
        if not isinstance(raw_profiles, dict):
            return defaults
        out = json.loads(json.dumps(defaults, ensure_ascii=False))
        for mode, row in raw_profiles.items():
            if mode not in out or not isinstance(row, dict):
                continue
            if "team_size" in row:
                out[mode]["team_size"] = max(2, min(6, int(row.get("team_size", out[mode]["team_size"]))))
            if "verification_level" in row:
                level = str(row.get("verification_level", out[mode]["verification_level"])).strip().lower()
                if level in {"light", "standard", "strict"}:
                    out[mode]["verification_level"] = level
            if "max_tool_calls" in row:
                out[mode]["max_tool_calls"] = max(0, min(20, int(row.get("max_tool_calls", out[mode]["max_tool_calls"]))))
            if "step_timeout_seconds" in row:
                out[mode]["step_timeout_seconds"] = max(5, min(180, int(row.get("step_timeout_seconds", out[mode]["step_timeout_seconds"]))))
        return out

    def team_stage_roles(self) -> Dict[str, str]:
        team = self.team_settings()
        raw = team.get("stage_roles", {})
        if not isinstance(raw, dict):
            return {}
        out: Dict[str, str] = {}
        for key in ["planner", "solver", "verifier", "strict_checker", "synthesizer"]:
            value = str(raw.get(key, "auto")).strip()
            out[key] = value or "auto"
        return out

    def team_step_timeout_seconds(self) -> int:
        team = self.team_settings()
        return max(5, int(team.get("step_timeout_seconds", 45)))

    def team_rate_limit_cooldown_seconds(self) -> int:
        team = self.team_settings()
        return max(5, min(600, int(team.get("rate_limit_cooldown_seconds", 75))))

    def team_prefer_provider_diversity(self) -> bool:
        team = self.team_settings()
        return bool(team.get("prefer_provider_diversity", True))

    def tools_autonomy_settings(self) -> Dict[str, Any]:
        raw = self.config.get("tools_autonomy", {})
        if isinstance(raw, dict):
            return dict(raw)
        return {}

    def tools_autonomy_max_tool_calls(self) -> int:
        cfg = self.tools_autonomy_settings()
        return max(0, min(20, int(cfg.get("max_tool_calls", 4))))

    def work_settings(self) -> Dict[str, Any]:
        work = self.config.get("work_execution", {})
        if isinstance(work, dict):
            return dict(work)
        return {}

    def admin_settings(self) -> Dict[str, Any]:
        raw = self.config.get("admin_portal", {})
        if isinstance(raw, dict):
            return dict(raw)
        return {}

    def admin_enabled(self) -> bool:
        cfg = self.admin_settings()
        return bool(cfg.get("enabled", True))

    def admin_require_basic_auth(self) -> bool:
        cfg = self.admin_settings()
        return bool(cfg.get("require_basic_auth", False))

    def admin_basic_auth_user(self) -> str:
        cfg = self.admin_settings()
        env_name = str(cfg.get("basic_auth_user_env", "GATEWAY_ADMIN_USER")).strip()
        if env_name:
            return os.getenv(env_name, "").strip()
        return ""

    def admin_basic_auth_pass(self) -> str:
        cfg = self.admin_settings()
        env_name = str(cfg.get("basic_auth_pass_env", "GATEWAY_ADMIN_PASS")).strip()
        if env_name:
            return os.getenv(env_name, "").strip()
        return ""

    def work_max_concurrency(self) -> int:
        settings = self.work_settings()
        return max(1, min(16, int(settings.get("max_concurrency", 2))))

    def work_retry_count(self) -> int:
        settings = self.work_settings()
        return max(0, min(5, int(settings.get("retry_count", 1))))

    def work_retry_backoff_seconds(self) -> int:
        settings = self.work_settings()
        return max(1, min(30, int(settings.get("retry_backoff_seconds", 2))))

    def work_approval_ttl_seconds(self) -> int:
        settings = self.work_settings()
        return max(30, min(86400, int(settings.get("approval_ttl_seconds", 900))))

    def work_approval_secret(self) -> str:
        settings = self.work_settings()
        env_name = str(settings.get("approval_hmac_secret_env", "GATEWAY_APPROVAL_HMAC_SECRET")).strip()
        if env_name:
            env_val = os.getenv(env_name, "").strip()
            if env_val:
                return env_val
        return self._ephemeral_approval_secret

    def privileged_actions_settings(self) -> Dict[str, Any]:
        settings = self.work_settings()
        raw = settings.get("privileged_actions", {})
        if isinstance(raw, dict):
            return dict(raw)
        return {}

    def privileged_actions_enabled(self) -> bool:
        cfg = self.privileged_actions_settings()
        return bool(cfg.get("enabled", False))

    def privileged_required_actions(self) -> set[str]:
        cfg = self.privileged_actions_settings()
        raw = cfg.get("required_actions", ["shell", "organize_apply", "approve"])
        if not isinstance(raw, list):
            return {"shell", "organize_apply", "approve"}
        out = {str(x).strip().lower() for x in raw if str(x).strip()}
        return out or {"shell", "organize_apply", "approve"}

    def privileged_api_keys(self) -> List[str]:
        cfg = self.privileged_actions_settings()
        keys: List[str] = []
        raw_keys = cfg.get("api_keys", [])
        if isinstance(raw_keys, list):
            keys.extend([str(x).strip() for x in raw_keys if str(x).strip()])
        env_name = str(cfg.get("api_keys_env", "GATEWAY_WORK_API_KEYS")).strip()
        if env_name:
            env_val = os.getenv(env_name, "").strip()
            if env_val:
                keys.extend([part.strip() for part in re.split(r"[,\n;]", env_val) if part.strip()])
        file_path = str(cfg.get("api_keys_file", "")).strip()
        if file_path:
            try:
                raw = _abs_path(file_path).read_text(encoding="utf-8")
                keys.extend([part.strip() for part in re.split(r"[,\n;]", raw) if part.strip()])
            except Exception:
                pass
        dedup: List[str] = []
        seen: set[str] = set()
        for item in keys:
            if item in seen:
                continue
            seen.add(item)
            dedup.append(item)
        return dedup

    def work_run_timeout_seconds(self) -> int:
        settings = self.work_settings()
        return max(30, min(1800, int(settings.get("run_timeout_seconds", 300))))

    def work_provider_retry(self, provider: str) -> Dict[str, int]:
        settings = self.work_settings()
        default_retry = self.work_retry_count()
        default_backoff = self.work_retry_backoff_seconds()
        provider_retry = settings.get("provider_retry", {})
        if not isinstance(provider_retry, dict):
            return {"retry_count": default_retry, "retry_backoff_seconds": default_backoff}
        item = provider_retry.get(provider, {})
        if not isinstance(item, dict):
            return {"retry_count": default_retry, "retry_backoff_seconds": default_backoff}
        return {
            "retry_count": max(0, min(5, int(item.get("retry_count", default_retry)))),
            "retry_backoff_seconds": max(1, min(30, int(item.get("retry_backoff_seconds", default_backoff)))),
        }

    def circuit_breaker_settings(self) -> Dict[str, Any]:
        settings = self.work_settings()
        cb = settings.get("circuit_breaker", {})
        if isinstance(cb, dict):
            return dict(cb)
        return {}

    def circuit_breaker_threshold(self) -> int:
        cb = self.circuit_breaker_settings()
        return max(1, min(20, int(cb.get("fail_threshold", 3))))

    def circuit_breaker_cooldown_seconds(self) -> int:
        cb = self.circuit_breaker_settings()
        return max(5, min(600, int(cb.get("cooldown_seconds", 60))))

    def _all_routes(self) -> List[Dict[str, Any]]:
        routing = self.config.get("routing", {})
        if not isinstance(routing, dict):
            return []
        routes = routing.get("routes", [])
        if not isinstance(routes, list):
            return []
        return [route for route in routes if isinstance(route, dict)]

    def _default_route_name(self) -> str:
        routing = self.config.get("routing", {})
        if not isinstance(routing, dict):
            return "balanced"
        v = str(routing.get("default_route", "balanced")).strip()
        return v or "balanced"

    def _route_by_name(self, route_name: str) -> Dict[str, Any]:
        for route in self._all_routes():
            if str(route.get("name", "")).strip() == route_name:
                return route
        default_name = self._default_route_name()
        for route in self._all_routes():
            if str(route.get("name", "")).strip() == default_name:
                return route
        raise RuntimeError("No route entries defined in gateway config")

    def _load_local_registry(self) -> Dict[str, Any]:
        local_registry = str(self.config.get("local_registry", "config/local_model_registry.json"))
        registry_path = _abs_path(local_registry)
        raw = registry_path.read_text(encoding="utf-8")
        data = json.loads(raw)
        if not isinstance(data, dict):
            raise RuntimeError(f"Local registry must be a JSON object: {registry_path}")
        return data

    def local_registry_path(self) -> Path:
        local_registry = str(self.config.get("local_registry", "config/local_model_registry.json"))
        return _abs_path(local_registry)

    def save_local_registry(self, data: Dict[str, Any]) -> None:
        if not isinstance(data, dict):
            raise RuntimeError("Local registry must be a JSON object")
        path = self.local_registry_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(path)

    def local_nodes(self) -> List[Dict[str, Any]]:
        registry = self._load_local_registry()
        cluster = registry.get("lm_studio_cluster", {})
        if not isinstance(cluster, dict):
            return []
        nodes = cluster.get("nodes", [])
        if not isinstance(nodes, list):
            return []
        out: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for node in nodes:
            if not isinstance(node, dict):
                continue
            base_url = str(node.get("base_url", "")).strip().rstrip("/")
            if not base_url or base_url in seen:
                continue
            seen.add(base_url)
            out.append(node)
        return out

    def _resolve_local_role(self, role: str) -> List[Backend]:
        registry = self._load_local_registry()
        cluster = registry.get("lm_studio_cluster", {})
        nodes = cluster.get("nodes", []) if isinstance(cluster, dict) else []

        out: List[Backend] = []
        seen: set[Tuple[str, str]] = set()
        for node in nodes:
            if not isinstance(node, dict):
                continue
            base_url = str(node.get("base_url", "")).strip().rstrip("/")
            if not base_url:
                continue
            for model in node.get("models", []):
                if not isinstance(model, dict):
                    continue
                model_id = str(model.get("id", "")).strip()
                model_role = str(model.get("role", "")).strip().lower()
                enabled = bool(model.get("enabled", True))
                if not model_id or model_role != role.lower():
                    continue
                if not enabled:
                    continue
                key = (base_url, model_id)
                if key in seen:
                    continue
                seen.add(key)
                host = base_url.replace("http://", "").replace("https://", "")
                out.append(
                    Backend(
                        name=f"local:{role}:{host}:{model_id}",
                        kind="local",
                        model=model_id,
                        base_url=base_url,
                        provider="local",
                        source_role=role,
                    )
                )
        return out

    def _pollinations_backend(self) -> Backend:
        poll = self.config.get("pollinations", {})
        if not isinstance(poll, dict):
            poll = {}
        base_url = str(poll.get("base_url", "https://text.pollinations.ai")).strip().rstrip("/")
        route_model = str(poll.get("route_model", "openai-fast")).strip() or "openai-fast"
        model = str(poll.get("model", "gpt-oss-20b")).strip() or "gpt-oss-20b"
        api_key_env = str(poll.get("api_key_env", "POLLINATIONS_API_KEY")).strip() or "POLLINATIONS_API_KEY"
        return Backend(
            name=f"pollinations:{route_model}:{model}",
            kind="pollinations",
            model=model,
            base_url=base_url,
            provider="pollinations",
            route_model=route_model,
            api_key_env=api_key_env,
        )

    def _named_openai_backend(self, backend_name: str) -> Backend:
        backends = self.config.get("backends", {})
        if not isinstance(backends, dict):
            raise RuntimeError(f"Named backend '{backend_name}' was not found")
        cfg = backends.get(backend_name, {})
        if not isinstance(cfg, dict):
            raise RuntimeError(f"Named backend '{backend_name}' is not valid")
        if not bool(cfg.get("enabled", True)):
            raise RuntimeError(f"Named backend '{backend_name}' is disabled")

        kind = str(cfg.get("kind", "openai")).strip().lower()
        if kind != "openai":
            raise RuntimeError(f"Named backend '{backend_name}' has unsupported kind '{kind}'")

        base_url = str(cfg.get("base_url", "")).strip().rstrip("/")
        model = str(cfg.get("model", "")).strip()
        if not base_url or not model:
            raise RuntimeError(f"Named backend '{backend_name}' is missing base_url/model")

        inferred_provider = str(cfg.get("provider", "")).strip().lower()
        if not inferred_provider:
            if "gemini" in backend_name.lower():
                inferred_provider = "gemini"
            elif "groq" in backend_name.lower():
                inferred_provider = "groq"
            elif "mistral" in backend_name.lower():
                inferred_provider = "mistral"
            elif "cerebras" in backend_name.lower():
                inferred_provider = "cerebras"
            elif "openrouter" in backend_name.lower():
                inferred_provider = "openrouter"
            else:
                inferred_provider = "openai"

        extra_headers: Dict[str, str] = {}
        raw_headers = cfg.get("extra_headers", {})
        if isinstance(raw_headers, dict):
            for key, value in raw_headers.items():
                k = str(key).strip()
                v = str(value).strip()
                if k and v:
                    extra_headers[k] = v

        return Backend(
            name=f"openai:{backend_name}:{model}",
            kind="openai",
            model=model,
            base_url=base_url,
            provider=inferred_provider,
            api_key_env=str(cfg.get("api_key_env", "")).strip(),
            api_key_file=str(cfg.get("api_key_file", "")).strip(),
            extra_headers=extra_headers,
        )

    def _message_text(self, messages: List[Dict[str, Any]]) -> str:
        parts: List[str] = []
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            content = msg.get("content", "")
            if isinstance(content, str):
                parts.append(content)
            elif isinstance(content, list):
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        txt = str(part.get("text", "")).strip()
                        if txt:
                            parts.append(txt)
        return "\n".join(parts).lower()

    def pick_route_name(self, body: Dict[str, Any]) -> str:
        messages = body.get("messages", [])
        if not isinstance(messages, list):
            messages = []
        text = self._message_text(messages)

        metadata = body.get("metadata", {})
        if isinstance(metadata, dict):
            hint = str(metadata.get("router_hint", "")).strip()
            if hint:
                route_names = {str(route.get("name", "")).strip() for route in self._all_routes()}
                if hint in route_names:
                    return hint

        default_route = self._default_route_name()
        for route in self._all_routes():
            name = str(route.get("name", "")).strip()
            if not name or name == default_route:
                continue
            keywords = route.get("keywords", [])
            if not isinstance(keywords, list):
                continue
            for kw in keywords:
                kw_text = str(kw).strip().lower()
                if kw_text and kw_text in text:
                    return name
        return default_route

    def backends_for_route(self, route_name: str) -> List[Backend]:
        route = self._route_by_name(route_name)
        order = route.get("order", [])
        if not isinstance(order, list):
            return []

        out: List[Backend] = []
        for entry in order:
            if not isinstance(entry, dict):
                continue
            entry_type = str(entry.get("type", "")).strip().lower()
            if entry_type == "local_role":
                role = str(entry.get("role", "")).strip()
                if role:
                    out.extend(self._resolve_local_role(role))
            elif entry_type == "pollinations":
                out.append(self._pollinations_backend())
            elif entry_type == "openai":
                named = str(entry.get("backend", "")).strip()
                if named:
                    try:
                        out.append(self._named_openai_backend(named))
                    except Exception:
                        continue
        return out


class ClusterState:
    def __init__(self, router: GatewayRouter, event_logger: Optional["GatewayEventLogger"] = None) -> None:
        self.router = router
        self.event_logger = event_logger
        self._lock = threading.Lock()
        self._backend_status: Dict[str, Dict[str, Any]] = {}
        self._lm_discovery: Dict[str, Dict[str, Any]] = {}
        self._last_probe_ts = 0.0
        self._provider_failures: Dict[str, int] = {}
        self._provider_circuit_open_until: Dict[str, float] = {}
        self._provider_rate_limited_until: Dict[str, float] = {}
        self._provider_last_success: Dict[str, int] = {}
        self._provider_health_history: Dict[str, List[Dict[str, Any]]] = {}

    def reset(self) -> None:
        with self._lock:
            self._backend_status = {}
            self._lm_discovery = {}
            self._last_probe_ts = 0.0
            self._provider_failures = {}
            self._provider_circuit_open_until = {}
            self._provider_rate_limited_until = {}
            self._provider_last_success = {}
            self._provider_health_history = {}

    def _record_provider_health_locked(
        self,
        provider: str,
        ok: bool,
        source: str,
        backend_name: str,
        latency_ms: Optional[int],
        error: str,
    ) -> None:
        p = str(provider).strip().lower()
        if not p:
            return
        now = int(time.time())
        if ok:
            self._provider_last_success[p] = now
        bucket = self._provider_health_history.get(p, [])
        bucket.append(
            {
                "ts": now,
                "ok": bool(ok),
                "source": str(source).strip() or "runtime",
                "backend": str(backend_name).strip(),
                "latency_ms": int(latency_ms) if latency_ms is not None else None,
                "error": str(error).strip()[:180],
            }
        )
        if len(bucket) > 40:
            bucket = bucket[-40:]
        self._provider_health_history[p] = bucket

    def mark_runtime_result(self, backend: Backend, ok: bool, error: str = "", latency_ms: Optional[int] = None) -> None:
        now = time.time()
        payload = {
            "name": backend.name,
            "kind": backend.kind,
            "base_url": backend.base_url,
            "model": backend.model,
            "healthy": bool(ok),
            "error": error,
            "last_checked": int(now),
            "latency_ms": latency_ms,
            "source": "runtime",
        }
        with self._lock:
            self._backend_status[backend.name] = payload
            provider = str(backend.provider).strip().lower()
            if provider:
                self._record_provider_health_locked(
                    provider=provider,
                    ok=ok,
                    source="runtime",
                    backend_name=backend.name,
                    latency_ms=latency_ms,
                    error=error,
                )
                if ok:
                    self._provider_failures[provider] = 0
                    self._provider_circuit_open_until.pop(provider, None)
                else:
                    current = int(self._provider_failures.get(provider, 0)) + 1
                    self._provider_failures[provider] = current
                    threshold = self.router.circuit_breaker_threshold()
                    if current >= threshold:
                        cooldown = self.router.circuit_breaker_cooldown_seconds()
                        self._provider_circuit_open_until[provider] = time.time() + cooldown
        if self.event_logger is not None:
            self.event_logger.log(
                "provider_runtime",
                {
                    "backend": backend.name,
                    "provider": backend.provider,
                    "kind": backend.kind,
                    "ok": bool(ok),
                    "latency_ms": int(latency_ms) if latency_ms is not None else None,
                    "error": str(error)[:300],
                },
            )

    def is_provider_circuit_open(self, provider: str) -> bool:
        p = str(provider).strip().lower()
        if not p:
            return False
        with self._lock:
            until = float(self._provider_circuit_open_until.get(p, 0.0))
            if until <= 0:
                return False
            if time.time() >= until:
                self._provider_circuit_open_until.pop(p, None)
                self._provider_failures[p] = 0
                return False
            return True

    def mark_provider_rate_limited(self, provider: str, cooldown_seconds: int, source: str = "runtime") -> None:
        p = str(provider).strip().lower()
        if not p:
            return
        cooldown = max(5, min(600, int(cooldown_seconds)))
        until = time.time() + cooldown
        with self._lock:
            current = float(self._provider_rate_limited_until.get(p, 0.0))
            if until > current:
                self._provider_rate_limited_until[p] = until
                self._record_provider_health_locked(
                    provider=p,
                    ok=False,
                    source=str(source).strip() or "runtime",
                    backend_name=f"provider:{p}",
                    latency_ms=None,
                    error=f"rate_limited_cooldown_{cooldown}s",
                )

    def provider_rate_limited_until(self, provider: str) -> float:
        p = str(provider).strip().lower()
        if not p:
            return 0.0
        with self._lock:
            until = float(self._provider_rate_limited_until.get(p, 0.0))
            if until <= 0:
                return 0.0
            if time.time() >= until:
                self._provider_rate_limited_until.pop(p, None)
                return 0.0
            return until

    def status_for_backend(self, backend_name: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            status = self._backend_status.get(backend_name)
            return dict(status) if isinstance(status, dict) else None

    def should_probe(self) -> bool:
        interval = self.router.heartbeat_interval_seconds()
        return (time.time() - self._last_probe_ts) >= interval

    def probe_if_due(self, force: bool = False) -> None:
        if not force and not self.should_probe():
            return
        timeout = self.router.health_timeout_seconds()

        known_backends: Dict[str, Backend] = {}
        for route in self.router._all_routes():
            name = str(route.get("name", "")).strip()
            if not name:
                continue
            for backend in self.router.backends_for_route(name):
                known_backends[backend.name] = backend

        def _probe_backend(backend: Backend) -> Dict[str, Any]:
            start = time.time()
            ok = False
            error = ""
            try:
                if backend.kind == "local":
                    resp = requests.get(backend.base_url.rstrip("/") + "/models", timeout=timeout)
                    resp.raise_for_status()
                    parsed = resp.json()
                    if isinstance(parsed, dict) and isinstance(parsed.get("data"), list):
                        model_ids = {str(m.get("id", "")).strip() for m in parsed.get("data", []) if isinstance(m, dict)}
                        ok = backend.model in model_ids if backend.model else True
                        if not ok:
                            error = f"Model not loaded: {backend.model}"
                    else:
                        error = "Invalid /models response"
                elif backend.kind == "pollinations":
                    # Pollinations does not expose a strict OpenAI /models endpoint.
                    ok = True
                elif backend.kind == "openai":
                    base = backend.base_url.rstrip("/")
                    # Some OpenAI-compatible providers (notably Gemini compatibility endpoints)
                    # do not implement GET /models reliably for heartbeat probing.
                    if "generativelanguage.googleapis.com" in base and base.endswith("/openai"):
                        ok = True
                        error = "models_probe_skipped_openai_compat"
                    else:
                        headers = _auth_headers_for_backend(backend)
                        resp = requests.get(base + "/models", headers=headers, timeout=timeout)
                        resp.raise_for_status()
                        parsed = resp.json()
                        if isinstance(parsed, dict) and isinstance(parsed.get("data"), list):
                            ok = True
                        else:
                            error = "Invalid /models response"
                else:
                    error = f"Unsupported backend kind: {backend.kind}"
            except Exception as exc:
                ok = False
                error = str(exc)[:300]
            latency_ms = int((time.time() - start) * 1000)
            return {
                "name": backend.name,
                "kind": backend.kind,
                "provider": backend.provider,
                "base_url": backend.base_url,
                "model": backend.model,
                "healthy": ok,
                "error": error,
                "last_checked": int(time.time()),
                "latency_ms": latency_ms,
                "source": "heartbeat",
            }

        backend_status: Dict[str, Dict[str, Any]] = {}
        max_workers = max(4, min(32, len(known_backends) + len(self.router.local_nodes())))
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            backend_futures = {
                pool.submit(_probe_backend, backend): backend.name for backend in known_backends.values()
            }
            for future in concurrent.futures.as_completed(backend_futures):
                item = future.result()
                backend_status[str(item.get("name", ""))] = item

        def _discover_node(node: Dict[str, Any]) -> Optional[Tuple[str, Dict[str, Any]]]:
            base_url = str(node.get("base_url", "")).strip().rstrip("/")
            if not base_url:
                return None
            configured_models: List[str] = []
            for model in node.get("models", []):
                if isinstance(model, dict):
                    mid = str(model.get("id", "")).strip()
                    if mid:
                        configured_models.append(mid)

            item: Dict[str, Any] = {
                "base_url": base_url,
                "configured_models": configured_models,
                "loaded_models": [],
                "healthy": False,
                "error": "",
                "last_checked": int(time.time()),
            }
            try:
                start = time.time()
                resp = requests.get(base_url + "/models", timeout=timeout)
                resp.raise_for_status()
                parsed = resp.json()
                loaded_models: List[str] = []
                if isinstance(parsed, dict) and isinstance(parsed.get("data"), list):
                    for model in parsed.get("data", []):
                        if isinstance(model, dict):
                            mid = str(model.get("id", "")).strip()
                            if mid:
                                loaded_models.append(mid)
                item["loaded_models"] = loaded_models
                item["healthy"] = True
                item["latency_ms"] = int((time.time() - start) * 1000)
            except Exception as exc:
                item["error"] = str(exc)[:300]
            return base_url, item

        lm_discovery: Dict[str, Dict[str, Any]] = {}
        local_nodes = self.router.local_nodes()
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
            discovery_futures = {pool.submit(_discover_node, node): node for node in local_nodes}
            for future in concurrent.futures.as_completed(discovery_futures):
                result = future.result()
                if result is None:
                    continue
                base_url, item = result
                lm_discovery[base_url] = item

        with self._lock:
            self._backend_status = backend_status
            self._lm_discovery = lm_discovery
            self._last_probe_ts = time.time()
            for backend_name, item in backend_status.items():
                if not isinstance(item, dict):
                    continue
                provider = str(item.get("provider", "")).strip().lower()
                if not provider and backend_name.endswith("_primary"):
                    provider = _infer_provider_from_name(backend_name)
                if not provider:
                    continue
                self._record_provider_health_locked(
                    provider=provider,
                    ok=bool(item.get("healthy")),
                    source="heartbeat",
                    backend_name=backend_name,
                    latency_ms=item.get("latency_ms"),
                    error=str(item.get("error", "")),
                )

    def _provider_health_snapshot_locked(self) -> Dict[str, Dict[str, Any]]:
        now = int(time.time())
        out: Dict[str, Dict[str, Any]] = {}
        providers: set[str] = set(self._provider_failures.keys()) | set(self._provider_last_success.keys()) | set(
            self._provider_health_history.keys()
        ) | set(self._provider_circuit_open_until.keys()) | set(self._provider_rate_limited_until.keys())
        for provider in providers:
            open_until = int(self._provider_circuit_open_until.get(provider, 0))
            rate_limited_until = int(self._provider_rate_limited_until.get(provider, 0))
            out[provider] = {
                "provider": provider,
                "failures": int(self._provider_failures.get(provider, 0)),
                "circuit_open": open_until > now,
                "open_until_epoch": open_until,
                "rate_limited_cooldown_open": rate_limited_until > now,
                "rate_limited_until_epoch": rate_limited_until,
                "last_success_epoch": int(self._provider_last_success.get(provider, 0)),
                "recent": [dict(x) for x in self._provider_health_history.get(provider, [])[-12:]],
            }
        return out

    def provider_health_snapshot(self) -> Dict[str, Dict[str, Any]]:
        with self._lock:
            return self._provider_health_snapshot_locked()

    def status_payload(self) -> Dict[str, Any]:
        with self._lock:
            healthy = 0
            degraded = 0
            down = 0
            backends: List[Dict[str, Any]] = []
            for item in self._backend_status.values():
                b = dict(item)
                backends.append(b)
                if b.get("healthy") is True:
                    healthy += 1
                elif b.get("error"):
                    down += 1
                else:
                    degraded += 1

            return {
                "ok": True,
                "gateway_model": self.router.gateway_model(),
                "heartbeat_interval_seconds": self.router.heartbeat_interval_seconds(),
                "last_probe_epoch": int(self._last_probe_ts),
                "summary": {
                    "healthy": healthy,
                    "down": down,
                    "degraded": degraded,
                    "total": len(backends),
                },
                "backends": sorted(backends, key=lambda x: str(x.get("name", ""))),
                "lmstudio": sorted(self._lm_discovery.values(), key=lambda x: str(x.get("base_url", ""))),
                "provider_circuit_breakers": {
                    provider: {"open_until_epoch": int(until), "failures": int(self._provider_failures.get(provider, 0))}
                    for provider, until in self._provider_circuit_open_until.items()
                    if until > time.time()
                },
                "provider_rate_limit_cooldowns": {
                    provider: {"open_until_epoch": int(until)}
                    for provider, until in self._provider_rate_limited_until.items()
                    if until > time.time()
                },
                "provider_health": self._provider_health_snapshot_locked(),
            }


class RunStore:
    def __init__(self, persist_dir: Path, event_logger: Optional["GatewayEventLogger"] = None) -> None:
        self._lock = threading.Lock()
        self._runs: Dict[str, Dict[str, Any]] = {}
        self._events: Dict[str, List[Dict[str, Any]]] = {}
        self._cancel_requested: set[str] = set()
        self.event_logger = event_logger
        self.persist_dir = persist_dir
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        self.runs_file = self.persist_dir / "runs.json"
        self.events_file = self.persist_dir / "events.jsonl"
        self._load()

    def _load(self) -> None:
        if self.runs_file.exists():
            try:
                loaded = json.loads(self.runs_file.read_text(encoding="utf-8"))
                if isinstance(loaded, dict):
                    self._runs = {str(k): v for k, v in loaded.items() if isinstance(v, dict)}
            except Exception:
                self._runs = {}
        if self.events_file.exists():
            try:
                for line in self.events_file.read_text(encoding="utf-8").splitlines():
                    if not line.strip():
                        continue
                    item = json.loads(line)
                    if not isinstance(item, dict):
                        continue
                    run_id = str(item.get("run_id", "")).strip()
                    if not run_id:
                        continue
                    self._events.setdefault(run_id, []).append(item)
            except Exception:
                self._events = {}

    def _save_runs_locked(self) -> None:
        tmp = self.runs_file.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._runs, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self.runs_file)

    def create_run(self, run: Dict[str, Any]) -> None:
        with self._lock:
            self._runs[str(run["id"])] = dict(run)
            self._events[str(run["id"])] = []
            self._save_runs_locked()
        if self.event_logger is not None:
            self.event_logger.log(
                "run_created",
                {"run_id": str(run.get("id", "")), "mode": str(run.get("mode", "")), "project_id": str(run.get("project_id", ""))},
            )

    def append_event(self, run_id: str, event_type: str, payload: Dict[str, Any]) -> None:
        item = {
            "run_id": run_id,
            "ts": int(time.time()),
            "type": event_type,
            "payload": payload,
        }
        with self._lock:
            bucket = self._events.get(run_id, [])
            bucket.append(item)
            self._events[run_id] = bucket
            with self.events_file.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")
        if self.event_logger is not None:
            self.event_logger.log(
                "run_event",
                {"run_id": run_id, "event_type": event_type, "payload": payload},
            )

    def update_run(self, run_id: str, patch: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        with self._lock:
            run = self._runs.get(run_id)
            if not isinstance(run, dict):
                return None
            run.update(patch)
            self._runs[run_id] = run
            self._save_runs_locked()
            out = dict(run)
        if self.event_logger is not None:
            self.event_logger.log(
                "run_updated",
                {"run_id": run_id, "patch_keys": sorted(list(patch.keys()))[:20], "status": str(out.get("status", ""))},
            )
        return out

    def request_cancel(self, run_id: str) -> None:
        with self._lock:
            self._cancel_requested.add(run_id)

    def is_cancel_requested(self, run_id: str) -> bool:
        with self._lock:
            return run_id in self._cancel_requested

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            run = self._runs.get(run_id)
            return dict(run) if isinstance(run, dict) else None

    def get_events(self, run_id: str) -> List[Dict[str, Any]]:
        with self._lock:
            return [dict(event) for event in self._events.get(run_id, [])]

    def list_runs(self, limit: int = 50) -> List[Dict[str, Any]]:
        lim = max(1, min(500, int(limit)))
        with self._lock:
            rows = [dict(v) for v in self._runs.values() if isinstance(v, dict)]
        rows.sort(key=lambda item: int(item.get("created_at", 0)), reverse=True)
        return rows[:lim]

    def claim_pending_approval(self, run_id: str, token_id: str, now_ts: int) -> Tuple[str, Optional[Dict[str, Any]]]:
        with self._lock:
            run = self._runs.get(run_id)
            if not isinstance(run, dict):
                return "run_missing", None
            pending = run.get("pending_approval", {})
            if not isinstance(pending, dict):
                return "no_pending", None
            used_ids_raw = run.get("used_approval_ids", [])
            used_ids = [str(x).strip() for x in used_ids_raw if str(x).strip()] if isinstance(used_ids_raw, list) else []
            if token_id in used_ids:
                return "used", None
            expires_at = int(pending.get("expires_at", 0))
            if expires_at > 0 and now_ts > expires_at:
                run["pending_approval"] = None
                run["updated_at"] = now_ts
                self._runs[run_id] = run
                self._save_runs_locked()
                return "expired", None
            expected_token_id = str(pending.get("token_id", "")).strip()
            if not token_id or not expected_token_id or token_id != expected_token_id:
                return "mismatch", None
            used_ids.append(token_id)
            run["used_approval_ids"] = used_ids[-20:]
            run["pending_approval"] = None
            run["updated_at"] = now_ts
            self._runs[run_id] = run
            self._save_runs_locked()
            return "claimed", dict(pending)


class WorkExecutor:
    def __init__(self, max_workers: int) -> None:
        self.max_workers = max_workers
        self._pool = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="work-run")
        self._lock = threading.Lock()
        self._pending = 0

    def submit(self, fn: Any, *args: Any, **kwargs: Any) -> None:
        with self._lock:
            self._pending += 1

        def _wrapped() -> None:
            try:
                fn(*args, **kwargs)
            finally:
                with self._lock:
                    self._pending = max(0, self._pending - 1)

        self._pool.submit(_wrapped)

    def pending(self) -> int:
        with self._lock:
            return self._pending


class ProjectMemoryStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._data: Dict[str, Dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            self._data = {}
            return
        try:
            parsed = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(parsed, dict):
                self._data = {str(k): v for k, v in parsed.items() if isinstance(v, dict)}
            else:
                self._data = {}
        except Exception:
            self._data = {}

    def _save_locked(self) -> None:
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        tmp.replace(self.path)

    def recall(self, project_id: str, limit: int = 8) -> Dict[str, List[str]]:
        pid = str(project_id).strip()
        with self._lock:
            item = self._data.get(pid, {})
            if not isinstance(item, dict):
                return {"facts": [], "decisions": [], "open_questions": [], "next_actions": []}
            out: Dict[str, List[str]] = {}
            for key in ["facts", "decisions", "open_questions", "next_actions"]:
                raw = item.get(key, [])
                if isinstance(raw, list):
                    out[key] = [str(x).strip() for x in raw if str(x).strip()][:limit]
                else:
                    out[key] = []
            return out

    def append_updates(
        self,
        project_id: str,
        objective: str,
        memory_updates: List[str],
        next_actions: List[str],
        open_questions: Optional[List[str]] = None,
    ) -> None:
        pid = str(project_id).strip()
        if not pid:
            return
        with self._lock:
            item = self._data.get(pid, {})
            if not isinstance(item, dict):
                item = {"facts": [], "decisions": [], "open_questions": [], "next_actions": []}
            for key in ["facts", "decisions", "open_questions", "next_actions"]:
                if not isinstance(item.get(key), list):
                    item[key] = []
            if objective.strip():
                item["facts"].append(f"Objective: {objective.strip()[:240]}")
            for m in memory_updates[:8]:
                txt = str(m).strip()
                if txt:
                    item["decisions"].append(txt[:260])
            for n in next_actions[:8]:
                txt = str(n).strip()
                if txt:
                    item["next_actions"].append(txt[:260])
            for q in (open_questions or [])[:6]:
                txt = str(q).strip()
                if txt:
                    item["open_questions"].append(txt[:260])
            for key in ["facts", "decisions", "open_questions", "next_actions"]:
                dedup: List[str] = []
                seen: set[str] = set()
                for v in reversed(item.get(key, [])):
                    s = str(v).strip()
                    if not s or s in seen:
                        continue
                    seen.add(s)
                    dedup.append(s)
                    if len(dedup) >= 40:
                        break
                item[key] = list(reversed(dedup))
            item["updated_at"] = int(time.time())
            self._data[pid] = item
            self._save_locked()


class GatewayEventLogger:
    def __init__(self, path: Path, enabled: bool = True, max_payload_chars: int = 4000, redact_secrets: bool = False) -> None:
        self.path = path
        self.enabled = bool(enabled)
        raw_chars = int(max_payload_chars)
        self.max_payload_chars = 0 if raw_chars <= 0 else max(200, min(5000000, raw_chars))
        self.redact_secrets = bool(redact_secrets)
        self._lock = threading.Lock()
        if self.enabled:
            self.path.parent.mkdir(parents=True, exist_ok=True)

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    def _clip(self, value: Any) -> Any:
        if self.max_payload_chars <= 0:
            if isinstance(value, list):
                return [self._clip(item) for item in value]
            if isinstance(value, dict):
                return {str(key): self._clip(value.get(key)) for key in value.keys()}
            return value
        if isinstance(value, str):
            return value[: self.max_payload_chars]
        if isinstance(value, list):
            out: List[Any] = []
            for item in value[:40]:
                out.append(self._clip(item))
            return out
        if isinstance(value, dict):
            out: Dict[str, Any] = {}
            for idx, key in enumerate(value.keys()):
                if idx >= 60:
                    break
                k = str(key)
                out[k] = self._clip(value.get(key))
            return out
        return value

    def _redact(self, value: Any, key_hint: str = "") -> Any:
        if not self.redact_secrets:
            return value
        key_low = str(key_hint).strip().lower()
        if any(tok in key_low for tok in ["api_key", "authorization", "token", "password", "secret"]):
            return "***redacted***"
        if isinstance(value, dict):
            return {str(key): self._redact(value.get(key), key_hint=str(key)) for key in value.keys()}
        if isinstance(value, list):
            return [self._redact(item, key_hint=key_hint) for item in value]
        if isinstance(value, str) and key_low in {"authorization"}:
            return "***redacted***"
        return value

    def log(self, event_type: str, payload: Dict[str, Any], level: str = "info") -> None:
        if not self.enabled:
            return
        item = {
            "ts": self._now_iso(),
            "epoch": int(time.time()),
            "level": str(level).strip().lower() or "info",
            "service": "model-cluster-gateway",
            "event_type": str(event_type).strip() or "event",
            "payload": self._clip(self._redact(payload if isinstance(payload, dict) else {"raw": str(payload)})),
        }
        with self._lock:
            with self.path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(item, ensure_ascii=False) + "\n")

    def read_events(
        self,
        limit: int = 200,
        event_type: str = "",
        level: str = "",
        request_id: str = "",
        run_id: str = "",
    ) -> List[Dict[str, Any]]:
        if not self.enabled or not self.path.exists():
            return []
        lim = max(1, min(2000, int(limit)))
        et = str(event_type).strip().lower()
        lv = str(level).strip().lower()
        rid = str(request_id).strip()
        run = str(run_id).strip()
        with self._lock:
            try:
                lines = deque(self.path.read_text(encoding="utf-8", errors="replace").splitlines(), maxlen=5000)
            except Exception:
                return []
        out: List[Dict[str, Any]] = []
        for line in reversed(lines):
            if not line.strip():
                continue
            try:
                item = json.loads(line)
            except Exception:
                continue
            if not isinstance(item, dict):
                continue
            payload = item.get("payload", {})
            if not isinstance(payload, dict):
                payload = {}
            if et and str(item.get("event_type", "")).strip().lower() != et:
                continue
            if lv and str(item.get("level", "")).strip().lower() != lv:
                continue
            if rid and str(payload.get("request_id", "")).strip() != rid:
                continue
            if run and str(payload.get("run_id", "")).strip() != run:
                continue
            out.append(item)
            if len(out) >= lim:
                break
        out.reverse()
        return out

    def export_events_text(
        self,
        fmt: str,
        limit: int = 200,
        event_type: str = "",
        level: str = "",
        request_id: str = "",
        run_id: str = "",
    ) -> Tuple[str, str]:
        events = self.read_events(limit=limit, event_type=event_type, level=level, request_id=request_id, run_id=run_id)
        export_format = str(fmt).strip().lower() or "jsonl"
        if export_format == "json":
            return json.dumps(events, ensure_ascii=False, indent=2) + "\n", "application/json; charset=utf-8"
        if export_format == "csv":
            lines = ["ts,epoch,level,event_type,request_id,run_id,payload_json"]
            for item in events:
                payload = item.get("payload", {})
                if not isinstance(payload, dict):
                    payload = {}
                row = [
                    str(item.get("ts", "")),
                    str(item.get("epoch", "")),
                    str(item.get("level", "")),
                    str(item.get("event_type", "")),
                    str(payload.get("request_id", "")),
                    str(payload.get("run_id", "")),
                    json.dumps(payload, ensure_ascii=False).replace('"', '""'),
                ]
                row = [f"\"{cell}\"" for cell in row]
                lines.append(",".join(row))
            return "\n".join(lines) + "\n", "text/csv; charset=utf-8"
        lines = [json.dumps(item, ensure_ascii=False) for item in events]
        return "\n".join(lines) + ("\n" if lines else ""), "application/x-ndjson; charset=utf-8"


def _pick_payload_fields(body: Dict[str, Any]) -> Dict[str, Any]:
    allowed = {
        "messages",
        "temperature",
        "top_p",
        "max_tokens",
        "frequency_penalty",
        "presence_penalty",
        "response_format",
        "seed",
        "stop",
    }
    out: Dict[str, Any] = {}
    for key in allowed:
        if key in body:
            out[key] = body[key]
    return out


def _extract_text_content(resp_json: Dict[str, Any]) -> str:
    try:
        content = resp_json["choices"][0]["message"]["content"]
    except Exception:
        return ""

    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts: List[str] = []
        for part in content:
            if isinstance(part, dict) and part.get("type") == "text":
                txt = str(part.get("text", "")).strip()
                if txt:
                    parts.append(txt)
        return "\n".join(parts)

    return ""


def _rank_team_stage_candidates(
    ordered_candidates: List[Backend],
    primary_backend_name: str,
    provider_usage: Dict[str, int],
    rate_limited_providers: set[str],
    provider_cooldown_until: Dict[str, float],
    now_epoch: Optional[float] = None,
) -> List[Backend]:
    now_ts = float(now_epoch if now_epoch is not None else time.time())
    deduped: List[Backend] = []
    seen: set[str] = set()
    for candidate in ordered_candidates:
        if candidate.name in seen:
            continue
        seen.add(candidate.name)
        deduped.append(candidate)

    indexed = list(enumerate(deduped))

    def _score(item: Tuple[int, Backend]) -> Tuple[int, int, int, int]:
        idx, backend = item
        provider = str(backend.provider).strip().lower()
        cooldown_until = float(provider_cooldown_until.get(provider, 0.0))
        cooldown_active = provider in rate_limited_providers and cooldown_until > now_ts
        usage = int(provider_usage.get(provider, 0))
        primary_penalty = 0 if backend.name == primary_backend_name else 1
        return (1 if cooldown_active else 0, usage, primary_penalty, idx)

    indexed.sort(key=_score)
    return [backend for _, backend in indexed]


def _provider_concentration_metrics(steps: List[Dict[str, Any]]) -> Dict[str, Any]:
    providers: List[str] = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        p = str(step.get("provider", "")).strip().lower()
        if p:
            providers.append(p)
    if not providers:
        return {
            "provider_usage": {},
            "unique_providers": 0,
            "stage_count": 0,
            "max_share": 0.0,
            "diversity_score": 0.0,
        }
    counts = Counter(providers)
    total = len(providers)
    unique = len(counts)
    max_share = max(counts.values()) / float(total)
    diversity_score = unique / float(total)
    return {
        "provider_usage": dict(sorted(counts.items())),
        "unique_providers": unique,
        "stage_count": total,
        "max_share": round(max_share, 4),
        "diversity_score": round(diversity_score, 4),
    }


def _build_openai_chat_response(gateway_model: str, content: str, upstream: Dict[str, Any], backend_name: str) -> Dict[str, Any]:
    response: Dict[str, Any] = {
        "id": f"chatcmpl-{uuid.uuid4().hex[:24]}",
        "object": "chat.completion",
        "created": int(time.time()),
        "model": gateway_model,
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": content},
                "finish_reason": "stop",
            }
        ],
        "system_fingerprint": backend_name,
    }
    usage = upstream.get("usage")
    if isinstance(usage, dict):
        response["usage"] = usage
    return response


def _auth_headers_for_backend(backend: Backend) -> Dict[str, str]:
    headers = {"Content-Type": "application/json"}
    token = ""
    if backend.api_key_env:
        token = os.getenv(backend.api_key_env, "").strip()
    if not token and backend.api_key_file:
        try:
            token = _abs_path(backend.api_key_file).read_text(encoding="utf-8").strip()
        except Exception:
            token = ""
    if token:
        headers["Authorization"] = f"Bearer {token}"
    for key, value in backend.extra_headers.items():
        if key and value:
            headers[str(key)] = str(value)
    return headers


def call_backend(backend: Backend, request_body: Dict[str, Any], timeout_seconds: int) -> Dict[str, Any]:
    payload = _pick_payload_fields(request_body)
    payload["model"] = backend.model

    if backend.kind == "local":
        url = backend.base_url.rstrip("/") + "/chat/completions"
        resp = requests.post(url, json=payload, timeout=timeout_seconds)
        resp.raise_for_status()
        parsed = resp.json()
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Unexpected local response from {backend.name}")
        return parsed

    if backend.kind == "pollinations":
        url = backend.base_url.rstrip("/") + f"/{backend.route_model}"
        headers = _auth_headers_for_backend(backend)
        headers.setdefault("User-Agent", "model-cluster-gateway/1.0")
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout_seconds)
        resp.raise_for_status()
        parsed = resp.json()
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Unexpected pollinations response from {backend.name}")
        return parsed

    if backend.kind == "openai":
        url = backend.base_url.rstrip("/") + "/chat/completions"
        headers = _auth_headers_for_backend(backend)
        resp = requests.post(url, json=payload, headers=headers, timeout=timeout_seconds)
        resp.raise_for_status()
        parsed = resp.json()
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Unexpected openai backend response from {backend.name}")
        return parsed

    raise RuntimeError(f"Unsupported backend kind: {backend.kind}")


def _infer_provider_from_name(name: str) -> str:
    low = str(name).strip().lower()
    if "gemini" in low:
        return "gemini"
    if "groq" in low:
        return "groq"
    if "mistral" in low:
        return "mistral"
    if "cerebras" in low:
        return "cerebras"
    if "openrouter" in low:
        return "openrouter"
    return "openai"


def _redacted_config_for_admin(config: Dict[str, Any]) -> Dict[str, Any]:
    out = json.loads(json.dumps(config, ensure_ascii=False))
    work = out.get("work_execution", {})
    if isinstance(work, dict):
        privileged = work.get("privileged_actions", {})
        if isinstance(privileged, dict) and "api_keys" in privileged:
            privileged["api_keys"] = ["***redacted***"]
            work["privileged_actions"] = privileged
        out["work_execution"] = work
    return out


class OpenAICompatibleHandler(BaseHTTPRequestHandler):
    router: GatewayRouter
    state: ClusterState
    run_store: RunStore
    work_executor: WorkExecutor
    memory_store: ProjectMemoryStore
    event_logger: GatewayEventLogger

    def _ensure_request_id(self) -> str:
        rid = str(getattr(self, "_request_id", "")).strip()
        if rid:
            return rid
        rid = f"req_{uuid.uuid4().hex[:12]}"
        setattr(self, "_request_id", rid)
        return rid

    def _send_json(self, status: int, body: Dict[str, Any], extra_headers: Optional[Dict[str, str]] = None) -> None:
        req_id = self._ensure_request_id()
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("x-request-id", req_id)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "authorization,content-type")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        if extra_headers:
            for key, value in extra_headers.items():
                self.send_header(key, value)
        self.end_headers()
        self.wfile.write(payload)
        self.event_logger.log(
            "http_response",
            {
                "request_id": req_id,
                "method": str(getattr(self, "command", "")),
                "path": str(getattr(self, "_route_path", "")),
                "status": int(status),
                "response_bytes": len(payload),
            },
        )
        if self.router.logging_capture_response_body():
            self.event_logger.log(
                "http_response_body",
                {
                    "request_id": req_id,
                    "method": str(getattr(self, "command", "")),
                    "path": str(getattr(self, "_route_path", "")),
                    "status": int(status),
                    "body": body,
                },
            )

    def _is_admin_route(self, route_path: str) -> bool:
        return route_path == "/admin" or route_path.startswith("/-/admin")

    def _check_admin_access(self, route_path: str) -> Tuple[bool, Optional[Tuple[int, Dict[str, Any], Dict[str, str]]]]:
        if not self._is_admin_route(route_path):
            return True, None
        if not self.router.admin_enabled():
            return False, (
                404,
                {"error": {"message": "Admin portal is disabled", "type": "invalid_request_error"}},
                {},
            )
        if not self.router.admin_require_basic_auth():
            return True, None
        expected_user = self.router.admin_basic_auth_user()
        expected_pass = self.router.admin_basic_auth_pass()
        if not expected_user or not expected_pass:
            return False, (
                503,
                {"error": {"message": "Admin basic auth is enabled but credentials are not configured", "type": "server_error"}},
                {},
            )
        auth = str(self.headers.get("authorization", "")).strip()
        if not auth.lower().startswith("basic "):
            return False, (
                401,
                {"error": {"message": "Admin authentication required", "type": "invalid_request_error"}},
                {"WWW-Authenticate": 'Basic realm="model-cluster-gateway-admin"'},
            )
        raw = auth[6:].strip()
        try:
            decoded = base64.b64decode(raw).decode("utf-8", errors="strict")
        except Exception:
            return False, (
                401,
                {"error": {"message": "Invalid admin authentication", "type": "invalid_request_error"}},
                {"WWW-Authenticate": 'Basic realm="model-cluster-gateway-admin"'},
            )
        if ":" not in decoded:
            return False, (
                401,
                {"error": {"message": "Invalid admin authentication", "type": "invalid_request_error"}},
                {"WWW-Authenticate": 'Basic realm="model-cluster-gateway-admin"'},
            )
        user, pwd = decoded.split(":", 1)
        if not hmac.compare_digest(user, expected_user) or not hmac.compare_digest(pwd, expected_pass):
            return False, (
                401,
                {"error": {"message": "Invalid admin credentials", "type": "invalid_request_error"}},
                {"WWW-Authenticate": 'Basic realm="model-cluster-gateway-admin"'},
            )
        return True, None

    def _read_json(self) -> Dict[str, Any]:
        raw_len = self.headers.get("content-length", "0").strip()
        try:
            size = int(raw_len)
        except Exception:
            size = 0
        if size <= 0:
            return {}
        raw = self.rfile.read(size).decode("utf-8", errors="replace")
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            raise ValueError("Request body must be a JSON object")
        if self.router.logging_capture_request_body():
            self.event_logger.log(
                "http_request_body",
                {
                    "request_id": self._ensure_request_id(),
                    "method": str(getattr(self, "command", "")),
                    "path": str(getattr(self, "_route_path", "")),
                    "body": parsed,
                },
            )
        return parsed

    def _work_route(self, route_path: str) -> Tuple[bool, Optional[str], Optional[str]]:
        if route_path == "/v1/work":
            return True, None, None
        match = re.fullmatch(r"/v1/work/([A-Za-z0-9_-]+)(?:/(events|cancel|events/stream|approve))?", route_path)
        if not match:
            return False, None, None
        sub = match.group(2)
        if sub == "events/stream":
            sub = "events_stream"
        return True, match.group(1), sub

    def _extract_request_api_key(self) -> str:
        bearer = str(self.headers.get("authorization", "")).strip()
        if bearer.lower().startswith("bearer "):
            token = bearer[7:].strip()
            if token:
                return token
        return str(self.headers.get("x-api-key", "")).strip()

    def _is_privileged_action_allowed(self, action: str, request_api_key: str) -> Tuple[bool, str]:
        action_name = str(action).strip().lower()
        if not self.router.privileged_actions_enabled():
            return True, ""
        if action_name not in self.router.privileged_required_actions():
            return True, ""
        keys = self.router.privileged_api_keys()
        if not keys:
            return False, "privileged_auth_misconfigured"
        key = str(request_api_key).strip()
        if not key:
            return False, "privileged_auth_required"
        for allowed in keys:
            if hmac.compare_digest(key, allowed):
                return True, ""
        return False, "privileged_auth_required"

    def _make_approval_token(self, run_id: str, expires_at: int) -> Tuple[str, str]:
        token_id = uuid.uuid4().hex
        payload = {"v": 1, "rid": run_id, "exp": int(expires_at), "jti": token_id}
        payload_json = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        payload_b64 = _b64url_encode(payload_json.encode("utf-8"))
        secret = self.router.work_approval_secret().encode("utf-8")
        sig = hmac.new(secret, payload_b64.encode("utf-8"), hashlib.sha256).digest()
        token = f"ap1.{payload_b64}.{_b64url_encode(sig)}"
        return token, token_id

    def _verify_approval_token(self, token: str, run_id: str) -> Tuple[Optional[Dict[str, Any]], str]:
        raw = str(token).strip()
        parts = raw.split(".")
        if len(parts) != 3 or parts[0] != "ap1":
            return None, "invalid_token_format"
        payload_b64 = parts[1]
        sig_b64 = parts[2]
        secret = self.router.work_approval_secret().encode("utf-8")
        expected = _b64url_encode(hmac.new(secret, payload_b64.encode("utf-8"), hashlib.sha256).digest())
        if not hmac.compare_digest(sig_b64, expected):
            return None, "invalid_token_signature"
        try:
            payload_raw = _b64url_decode(payload_b64).decode("utf-8", errors="strict")
            payload = json.loads(payload_raw)
        except Exception:
            return None, "invalid_token_payload"
        if not isinstance(payload, dict):
            return None, "invalid_token_payload"
        if str(payload.get("rid", "")).strip() != str(run_id).strip():
            return None, "token_run_mismatch"
        exp = int(payload.get("exp", 0))
        if exp > 0 and int(time.time()) > exp:
            return None, "token_expired"
        token_id = str(payload.get("jti", "")).strip()
        if not token_id:
            return None, "invalid_token_payload"
        return payload, ""

    def _extract_section_bullets(self, text: str, headings: List[str], max_items: int) -> List[str]:
        norm_lines = [line.rstrip() for line in text.splitlines()]
        section_start = -1
        target = {h.strip().lower() for h in headings if h.strip()}
        for idx, line in enumerate(norm_lines):
            marker = line.strip().lower().strip("#*` ").rstrip(":")
            if marker in target:
                section_start = idx + 1
                break
        if section_start < 0:
            return []

        out: List[str] = []
        for line in norm_lines[section_start:]:
            cleaned = line.strip()
            if not cleaned:
                if out:
                    break
                continue
            if cleaned.endswith(":") and len(cleaned) <= 80:
                break
            cleaned = re.sub(r"^\d+\.\s*", "", cleaned)
            cleaned = re.sub(r"^[-*]\s*", "", cleaned)
            cleaned = cleaned.strip()
            if len(cleaned) < 8:
                continue
            out.append(cleaned[:260])
            if len(out) >= max_items:
                break
        return out

    def _extract_next_actions(self, text: str) -> List[str]:
        section_items = self._extract_section_bullets(
            text,
            headings=["2) next steps", "next steps", "actions", "immediate next actions"],
            max_items=6,
        )
        if section_items:
            return section_items
        out: List[str] = []
        for line in text.splitlines():
            cleaned = line.strip()
            if not cleaned:
                continue
            cleaned = re.sub(r"^\d+\.\s*", "", cleaned)
            cleaned = re.sub(r"^[-*]\s*", "", cleaned)
            if len(cleaned) < 8:
                continue
            out.append(cleaned[:220])
            if len(out) >= 6:
                break
        return out

    def _citation_keywords(self, objective: str) -> set[str]:
        words = re.findall(r"[a-zA-Z]{4,}", objective.lower())
        return {w for w in words if w not in {"with", "from", "that", "this", "project", "model", "gateway"}}

    def _build_citations(self, tool_context: Dict[str, Any], objective: str) -> List[Dict[str, Any]]:
        keywords = self._citation_keywords(objective)
        citations: List[Dict[str, Any]] = []

        for item in tool_context.get("web", []):
            if not isinstance(item, dict):
                continue
            url = str(item.get("url", "")).strip()
            title = str(item.get("title", "")).strip()
            snippet = str(item.get("snippet", "")).strip().lower()
            if not url:
                continue
            score = 1
            if title:
                score += 1
            if keywords:
                overlap = sum(1 for k in keywords if k in snippet)
                score += min(4, overlap)
            citations.append({"url": url, "title": title, "score": score, "source": "web_fetch"})

        for item in tool_context.get("web_search", []):
            if not isinstance(item, dict):
                continue
            for hit in item.get("results", []):
                if not isinstance(hit, dict):
                    continue
                url = str(hit.get("url", "")).strip()
                title = str(hit.get("title", "")).strip()
                snippet = str(hit.get("snippet", "")).strip().lower()
                if not url:
                    continue
                score = 1
                if title:
                    score += 1
                if keywords:
                    overlap = sum(1 for k in keywords if k in snippet)
                    score += min(4, overlap)
                citations.append({"url": url, "title": title, "score": score, "source": "web_search"})

        citations.sort(key=lambda x: int(x.get("score", 0)), reverse=True)
        dedup: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for c in citations:
            url = str(c.get("url", "")).strip()
            if not url or url in seen:
                continue
            seen.add(url)
            dedup.append(c)
            if len(dedup) >= 8:
                break
        return dedup

    def _build_memory_prompt_section(self, memory: Dict[str, List[str]]) -> str:
        def _fmt(items: List[str]) -> str:
            if not items:
                return "- none"
            return "\n".join(f"- {i}" for i in items[:8])

        return (
            "Project memory context:\n"
            f"Facts:\n{_fmt(memory.get('facts', []))}\n\n"
            f"Decisions:\n{_fmt(memory.get('decisions', []))}\n\n"
            f"Open questions:\n{_fmt(memory.get('open_questions', []))}\n\n"
            f"Carried next actions:\n{_fmt(memory.get('next_actions', []))}"
        )

    def _build_work_prompt(
        self,
        mode: str,
        project_id: str,
        objective: str,
        constraints: List[str],
        artifacts: List[str],
        memory: Dict[str, List[str]],
        cognitive_depth: str,
    ) -> str:
        mode_txt = mode.lower()
        if mode_txt == "coding":
            style = "Focus on implementation strategy, test plan, and concrete patch steps."
        elif mode_txt == "organize":
            style = "Focus on file organization, safe restructuring, and reversible changes."
        else:
            style = "Focus on research findings, evidence quality, and decision-ready summary."
        depth_style = {
            "low": "Use concise reasoning. Optimize for speed and directness.",
            "standard": "Use balanced reasoning. Prioritize clear tradeoffs and one verification check.",
            "deep": "Use deliberate reasoning. Compare alternatives and perform a self-critique before finalizing.",
        }.get(cognitive_depth, "Use balanced reasoning.")

        constraints_txt = "\n".join(f"- {c}" for c in constraints) if constraints else "- none provided"
        artifacts_txt = "\n".join(f"- {a}" for a in artifacts) if artifacts else "- none provided"
        memory_txt = self._build_memory_prompt_section(memory)
        return (
            f"Project: {project_id}\n"
            f"Mode: {mode_txt}\n"
            f"Cognitive depth: {cognitive_depth}\n"
            f"Objective: {objective}\n\n"
            f"Constraints:\n{constraints_txt}\n\n"
            f"Known artifacts/inputs:\n{artifacts_txt}\n\n"
            f"{memory_txt}\n\n"
            f"Instructions:\n{style}\n"
            f"{depth_style}\n"
            "Return sections:\n"
            "1) Direct answer\n"
            "2) Next steps (3-6 bullets)\n"
            "3) Risks / unknowns\n"
            "4) Immediate validation check to run now\n"
            "5) Memory updates to store (2-6 bullets)\n"
            "6) Open questions (0-3 bullets)"
        )

    def _safe_read_files(self, paths: List[str], max_chars: int = 4000) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        ws_root = Path("/home/joe/codex/active/autonomous-projects").resolve()
        for item in paths[:8]:
            raw = str(item).strip()
            if not raw:
                continue
            try:
                p = Path(raw).expanduser()
                if not p.is_absolute():
                    p = ws_root / raw
                p = p.resolve()
            except Exception:
                out.append({"path": raw, "error": "invalid_path"})
                continue
            if not str(p).startswith(str(ws_root)):
                out.append({"path": raw, "error": "outside_workspace"})
                continue
            if not p.exists() or not p.is_file():
                out.append({"path": str(p), "error": "not_file"})
                continue
            try:
                text = p.read_text(encoding="utf-8", errors="replace")
                out.append({"path": str(p), "content": text[:max_chars], "truncated": len(text) > max_chars})
            except Exception as exc:
                out.append({"path": str(p), "error": str(exc)[:200]})
        return out

    def _safe_web_fetch(self, urls: List[str], timeout_seconds: int = 8) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for raw in urls[:5]:
            url = str(raw).strip()
            if not url.startswith(("http://", "https://")):
                out.append({"url": url, "error": "invalid_url"})
                continue
            try:
                resp = requests.get(url, timeout=timeout_seconds, headers={"User-Agent": "model-cluster-gateway/1.0"})
                txt = resp.text[:2000]
                title_match = re.search(r"<title>(.*?)</title>", resp.text, flags=re.IGNORECASE | re.DOTALL)
                title = title_match.group(1).strip()[:200] if title_match else ""
                out.append({"url": url, "status_code": resp.status_code, "title": title, "snippet": txt})
            except Exception as exc:
                out.append({"url": url, "error": str(exc)[:200]})
        return out

    def _safe_web_search(self, queries: List[str], timeout_seconds: int = 8) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for q in queries[:4]:
            query = str(q).strip()
            if not query:
                continue
            try:
                resp = requests.get(
                    "https://api.duckduckgo.com/",
                    params={"q": query, "format": "json", "no_html": 1, "skip_disambig": 1},
                    timeout=timeout_seconds,
                    headers={"User-Agent": "model-cluster-gateway/1.0"},
                )
                resp.raise_for_status()
                parsed = resp.json()
                hits: List[Dict[str, Any]] = []
                if isinstance(parsed, dict):
                    abstract_url = str(parsed.get("AbstractURL", "")).strip()
                    abstract_txt = str(parsed.get("AbstractText", "")).strip()
                    heading = str(parsed.get("Heading", "")).strip()
                    if abstract_url:
                        hits.append({"title": heading, "url": abstract_url, "snippet": abstract_txt[:400]})
                    for item in parsed.get("RelatedTopics", []):
                        if not isinstance(item, dict):
                            continue
                        if isinstance(item.get("Topics"), list):
                            for sub in item.get("Topics", []):
                                if isinstance(sub, dict):
                                    u = str(sub.get("FirstURL", "")).strip()
                                    t = str(sub.get("Text", "")).strip()
                                    if u and t:
                                        hits.append({"title": t[:120], "url": u, "snippet": t[:400]})
                        else:
                            u = str(item.get("FirstURL", "")).strip()
                            t = str(item.get("Text", "")).strip()
                            if u and t:
                                hits.append({"title": t[:120], "url": u, "snippet": t[:400]})
                        if len(hits) >= 5:
                            break
                out.append({"query": query, "results": hits[:5]})
            except Exception as exc:
                out.append({"query": query, "error": str(exc)[:200]})
        return out

    def _safe_shell_exec(self, command: str, allow_shell: bool) -> Dict[str, Any]:
        cmd = str(command).strip()
        if not cmd:
            return {"error": "empty_command"}
        if not allow_shell:
            return {"error": "shell_disabled"}
        try:
            parts = shlex.split(cmd)
        except Exception:
            return {"error": "invalid_shell_syntax"}
        if not parts:
            return {"error": "empty_command"}
        allowed_exact = {("ls",), ("pwd",), ("git", "status"), ("rg", "--files")}
        allowed_prefix = [("rg", "-n")]
        key = tuple(parts[:2]) if len(parts) >= 2 else (parts[0],)
        ok = key in allowed_exact or any(tuple(parts[: len(pref)]) == pref for pref in allowed_prefix)
        if not ok:
            return {"error": "command_not_allowlisted", "allowlisted_examples": ["ls", "pwd", "git status", "rg --files", "rg -n <pattern> <path>"]}
        try:
            proc = subprocess.run(parts, capture_output=True, text=True, timeout=8, cwd="/home/joe/codex/active/autonomous-projects")
            return {
                "command": cmd,
                "returncode": proc.returncode,
                "stdout": proc.stdout[:4000],
                "stderr": proc.stderr[:2000],
            }
        except Exception as exc:
            return {"error": str(exc)[:200], "command": cmd}

    def _tool_autonomy_enabled(self, body: Dict[str, Any]) -> bool:
        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict):
            return True
        raw = metadata.get("tools_autonomy", True)
        if isinstance(raw, dict):
            enabled = raw.get("enabled", True)
            return bool(enabled)
        if isinstance(raw, bool):
            return raw
        if isinstance(raw, str):
            low = raw.strip().lower()
            if low in {"true", "1", "on", "yes"}:
                return True
            if low in {"false", "0", "off", "no"}:
                return False
        return bool(raw)

    def _tool_autonomy_max_tool_calls(self, body: Dict[str, Any]) -> int:
        configured = self.router.tools_autonomy_max_tool_calls()
        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict):
            return configured
        raw = metadata.get("tools_autonomy")
        if isinstance(raw, dict) and "max_tool_calls" in raw:
            try:
                return max(0, min(20, int(raw.get("max_tool_calls", configured))))
            except Exception:
                return configured
        if "max_tool_calls" in metadata:
            try:
                return max(0, min(20, int(metadata.get("max_tool_calls", configured))))
            except Exception:
                return configured
        return configured

    def _tool_autonomy_instruction(self) -> str:
        return (
            "You may decide to use tools when needed.\n"
            "When a tool is needed, respond ONLY with one line:\n"
            "TOOL_REQUEST: {\"tool\":\"web_search|web_fetch|read_files|shell|memory_read\", ...}\n"
            "Valid payloads:\n"
            "- {\"tool\":\"web_search\",\"queries\":[\"...\"]}\n"
            "- {\"tool\":\"web_fetch\",\"urls\":[\"https://...\"]}\n"
            "- {\"tool\":\"read_files\",\"paths\":[\"relative/or/absolute/path\"],\"max_chars\":4000}\n"
            "- {\"tool\":\"shell\",\"command\":\"ls\"} (requires metadata.allow_shell=true)\n"
            "- {\"tool\":\"memory_read\",\"project_id\":\"optional-project-id\"}\n"
            "After TOOL_RESULT arrives, continue and provide the answer normally."
        )

    def _extract_tool_request(self, text: str) -> Optional[Dict[str, Any]]:
        def _parse_json_obj(tail_text: str) -> Optional[Dict[str, Any]]:
            tail = tail_text.strip()
            if tail.startswith("```"):
                tail = re.sub(r"^```(?:json)?\s*", "", tail, flags=re.IGNORECASE)
                tail = re.sub(r"\s*```$", "", tail)
                tail = tail.strip()
            start = tail.find("{")
            if start < 0:
                return None
            depth = 0
            in_str = False
            escaped = False
            end = -1
            for i, ch in enumerate(tail[start:], start=start):
                if in_str:
                    if escaped:
                        escaped = False
                    elif ch == "\\":
                        escaped = True
                    elif ch == '"':
                        in_str = False
                    continue
                if ch == '"':
                    in_str = True
                    continue
                if ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        end = i
                        break
            if end < 0:
                return None
            raw_json = tail[start : end + 1]
            try:
                parsed = json.loads(raw_json)
            except Exception:
                return None
            if not isinstance(parsed, dict):
                return None
            return parsed

        marker = "TOOL_REQUEST:"
        idx = text.find(marker)
        if idx >= 0:
            parsed = _parse_json_obj(text[idx + len(marker) :])
            if isinstance(parsed, dict):
                return parsed

        if "<|tool_calls_section_begin|>" in text or "<|tool_call_begin|>" in text:
            fn_match = re.search(r"functions\.([A-Za-z_][A-Za-z0-9_]*)\s*:\d+", text)
            if not fn_match:
                fn_match = re.search(r"functions\.([A-Za-z_][A-Za-z0-9_]*)", text)
            if not fn_match:
                return None
            fn_name = str(fn_match.group(1)).strip().lower()
            args_match = re.search(
                r"<\|tool_call_argument_begin\|>\s*(\{.*?\})\s*(?:<\|tool_call_end\|>|<\|tool_call_argument_end\|>)",
                text,
                flags=re.DOTALL,
            )
            args: Dict[str, Any] = {}
            if args_match:
                try:
                    parsed_args = json.loads(args_match.group(1))
                    if isinstance(parsed_args, dict):
                        args = parsed_args
                except Exception:
                    args = {}
            normalized_tool = fn_name.split(".")[-1]
            request: Dict[str, Any] = {"tool": normalized_tool}
            request.update(args)
            return request

        start = text.find("{")
        if start < 0:
            return None
        fallback = _parse_json_obj(text[start:])
        if isinstance(fallback, dict) and "tool" in fallback:
            return fallback
        return None

    def _execute_tool_request(self, request: Dict[str, Any], body: Dict[str, Any]) -> Dict[str, Any]:
        tool = str(request.get("tool", "")).strip().lower()
        self.event_logger.log(
            "tool_request",
            {"request_id": self._ensure_request_id(), "tool": tool, "request": request},
        )
        if tool == "web_search":
            queries_raw = request.get("queries", [])
            queries = [str(x).strip() for x in queries_raw if str(x).strip()] if isinstance(queries_raw, list) else []
            out = {"ok": True, "tool": "web_search", "results": self._safe_web_search(queries)}
            self.event_logger.log("tool_result", {"request_id": self._ensure_request_id(), "tool": tool, "ok": True, "result_count": len(out.get("results", []))})
            return out
        if tool == "web_fetch":
            urls_raw = request.get("urls", [])
            urls = [str(x).strip() for x in urls_raw if str(x).strip()] if isinstance(urls_raw, list) else []
            out = {"ok": True, "tool": "web_fetch", "results": self._safe_web_fetch(urls)}
            self.event_logger.log("tool_result", {"request_id": self._ensure_request_id(), "tool": tool, "ok": True, "result_count": len(out.get("results", []))})
            return out
        if tool == "read_files":
            paths_raw = request.get("paths", [])
            paths = [str(x).strip() for x in paths_raw if str(x).strip()] if isinstance(paths_raw, list) else []
            max_chars = request.get("max_chars", 4000)
            try:
                max_chars_i = max(200, min(15000, int(max_chars)))
            except Exception:
                max_chars_i = 4000
            out = {"ok": True, "tool": "read_files", "results": self._safe_read_files(paths, max_chars=max_chars_i)}
            self.event_logger.log("tool_result", {"request_id": self._ensure_request_id(), "tool": tool, "ok": True, "result_count": len(out.get("results", []))})
            return out
        if tool == "shell":
            cmd = str(request.get("command", "")).strip()
            metadata = body.get("metadata", {})
            if not isinstance(metadata, dict):
                metadata = {}
            allow_shell = bool(metadata.get("allow_shell", False))
            out = {"ok": True, "tool": "shell", "result": self._safe_shell_exec(cmd, allow_shell=allow_shell)}
            self.event_logger.log("tool_result", {"request_id": self._ensure_request_id(), "tool": tool, "ok": True})
            return out
        if tool == "memory_read":
            metadata = body.get("metadata", {})
            if not isinstance(metadata, dict):
                metadata = {}
            project_id = str(request.get("project_id", "")).strip() or str(metadata.get("project_id", "")).strip() or str(body.get("project_id", "")).strip() or "default-project"
            context = self.memory_store.recall(project_id)
            out = {"ok": True, "tool": "memory_read", "project_id": project_id, "result": context}
            self.event_logger.log("tool_result", {"request_id": self._ensure_request_id(), "tool": tool, "ok": True, "project_id": project_id})
            return out
        out = {"ok": False, "error": f"unsupported_tool:{tool}", "supported": ["web_search", "web_fetch", "read_files", "shell", "memory_read"]}
        self.event_logger.log("tool_result", {"request_id": self._ensure_request_id(), "tool": tool, "ok": False, "error": out["error"]}, level="warn")
        return out

    def _call_backend_with_tool_autonomy(
        self,
        backend: Backend,
        body: Dict[str, Any],
        messages: List[Dict[str, Any]],
        timeout_seconds: int,
        enable_tool_autonomy: bool,
    ) -> Tuple[str, int, List[Dict[str, Any]], Dict[str, Any]]:
        def _strip_tool_request_markers(text: str) -> str:
            out = re.sub(r"TOOL_REQUEST:\s*\{.*?\}\s*$", "", text, flags=re.DOTALL).strip()
            out = re.sub(r"<\|tool_calls_section_begin\|>.*?<\|tool_calls_section_end\|>", "", out, flags=re.DOTALL).strip()
            return out or text.strip()

        messages_work = [dict(msg) for msg in messages if isinstance(msg, dict)]
        if enable_tool_autonomy:
            messages_work = messages_work + [{"role": "system", "content": self._tool_autonomy_instruction()}]
        total_latency_ms = 0
        tool_trace: List[Dict[str, Any]] = []
        tool_calls_used = 0
        max_tool_calls = self._tool_autonomy_max_tool_calls(body)
        for tool_iter in range(max_tool_calls + 1):
            call_body = _pick_payload_fields(body)
            call_body["messages"] = messages_work
            if self.router.logging_include_model_transcripts():
                self.event_logger.log(
                    "model_call_input",
                    {
                        "request_id": self._ensure_request_id(),
                        "backend": backend.name,
                        "provider": backend.provider,
                        "tool_iteration": tool_iter,
                        "messages": messages_work,
                    },
                )
            start = time.time()
            upstream = call_backend(backend, call_body, timeout_seconds)
            text = _extract_text_content(upstream).strip()
            if not text:
                raise RuntimeError("Upstream returned empty assistant content")
            latency_ms = int((time.time() - start) * 1000)
            total_latency_ms += latency_ms
            self.state.mark_runtime_result(backend=backend, ok=True, latency_ms=latency_ms)
            if self.router.logging_include_model_transcripts():
                self.event_logger.log(
                    "model_call_output",
                    {
                        "request_id": self._ensure_request_id(),
                        "backend": backend.name,
                        "provider": backend.provider,
                        "tool_iteration": tool_iter,
                        "latency_ms": latency_ms,
                        "assistant_text": text,
                        "upstream": upstream,
                    },
                )
            if not enable_tool_autonomy:
                return text, total_latency_ms, tool_trace, {
                    "enabled": False,
                    "max_tool_calls": max_tool_calls,
                    "tool_calls_used": 0,
                    "tool_calls_remaining": max_tool_calls,
                    "loop_termination_reason": "tools_autonomy_disabled",
                }
            req = self._extract_tool_request(text)
            if req is None:
                return text, total_latency_ms, tool_trace, {
                    "enabled": True,
                    "max_tool_calls": max_tool_calls,
                    "tool_calls_used": tool_calls_used,
                    "tool_calls_remaining": max(0, max_tool_calls - tool_calls_used),
                    "loop_termination_reason": "assistant_finalized",
                }
            if tool_iter >= max_tool_calls:
                messages_work.append({"role": "assistant", "content": text})
                messages_work.append(
                    {
                        "role": "user",
                        "content": (
                            "TOOL_BUDGET_EXHAUSTED: You cannot request more tools.\n"
                            "Use the already returned tool results and provide your best final answer now.\n"
                            "Do not output TOOL_REQUEST."
                        ),
                    }
                )
                final_body = _pick_payload_fields(body)
                final_body["messages"] = messages_work
                final_start = time.time()
                final_upstream = call_backend(backend, final_body, timeout_seconds)
                final_text = _extract_text_content(final_upstream).strip()
                final_latency_ms = int((time.time() - final_start) * 1000)
                total_latency_ms += final_latency_ms
                self.state.mark_runtime_result(backend=backend, ok=True, latency_ms=final_latency_ms)
                final_text = _strip_tool_request_markers(final_text)
                return final_text, total_latency_ms, tool_trace, {
                    "enabled": True,
                    "max_tool_calls": max_tool_calls,
                    "tool_calls_used": tool_calls_used,
                    "tool_calls_remaining": max(0, max_tool_calls - tool_calls_used),
                    "loop_termination_reason": "tool_budget_exhausted",
                }
            tool_result = self._execute_tool_request(req, body)
            tool_calls_used += 1
            tool_trace.append({"request": req, "result": tool_result})
            messages_work.append({"role": "assistant", "content": text})
            messages_work.append(
                {
                    "role": "user",
                    "content": (
                        "TOOL_RESULT:\n"
                        f"{json.dumps(tool_result, ensure_ascii=False)}\n\n"
                        "Use this result and continue. If complete, provide final answer without TOOL_REQUEST."
                    ),
                }
            )
        raise RuntimeError("Tool autonomy loop failed")

    def _collect_tool_context(self, body: Dict[str, Any]) -> Dict[str, Any]:
        tools = body.get("tools", {})
        metadata = body.get("metadata", {})
        if not isinstance(tools, dict):
            tools = {}
        if not isinstance(metadata, dict):
            metadata = {}
        context: Dict[str, Any] = {"files": [], "web": [], "web_search": [], "shell": None}

        files = tools.get("files", {})
        if isinstance(files, dict):
            paths = files.get("paths", [])
            if isinstance(paths, list):
                max_chars = int(files.get("max_chars", 4000))
                context["files"] = self._safe_read_files(paths, max_chars=max(200, min(15000, max_chars)))

        web = tools.get("web", {})
        if isinstance(web, dict):
            urls = web.get("urls", [])
            if isinstance(urls, list):
                context["web"] = self._safe_web_fetch(urls)
            search_queries = web.get("search_queries", [])
            if isinstance(search_queries, list):
                context["web_search"] = self._safe_web_search(search_queries)

        shell = tools.get("shell", {})
        if isinstance(shell, dict):
            command = str(shell.get("command", "")).strip()
            allow_shell = bool(metadata.get("allow_shell", False))
            context["shell"] = self._safe_shell_exec(command, allow_shell=allow_shell)

        self.event_logger.log(
            "tool_context_collected",
            {
                "request_id": self._ensure_request_id(),
                "files": context.get("files", []),
                "web": context.get("web", []),
                "web_search": context.get("web_search", []),
                "shell": context.get("shell"),
            },
        )
        return context

    def _workspace_root(self) -> Path:
        return Path("/home/joe/codex/active/autonomous-projects").resolve()

    def _validate_workspace_path(self, raw_path: str) -> Tuple[Optional[Path], Optional[str]]:
        raw = str(raw_path).strip()
        if not raw:
            return None, "empty_path"
        try:
            p = Path(raw).expanduser()
            if not p.is_absolute():
                p = self._workspace_root() / raw
            p = p.resolve()
        except Exception:
            return None, "invalid_path"
        if not str(p).startswith(str(self._workspace_root())):
            return None, "outside_workspace"
        return p, None

    def _execute_organize_plan(self, plan: Dict[str, Any], apply_changes: bool) -> Dict[str, Any]:
        operations: List[Dict[str, Any]] = []
        changed: List[str] = []
        mkdirs = plan.get("mkdir", [])
        moves = plan.get("moves", [])
        if not isinstance(mkdirs, list):
            mkdirs = []
        if not isinstance(moves, list):
            moves = []

        for raw in mkdirs[:20]:
            target, err = self._validate_workspace_path(str(raw))
            if err:
                operations.append({"op": "mkdir", "target": str(raw), "ok": False, "error": err})
                continue
            if apply_changes:
                target.mkdir(parents=True, exist_ok=True)
                changed.append(str(target))
            operations.append({"op": "mkdir", "target": str(target), "ok": True, "applied": apply_changes})

        for mv in moves[:50]:
            if not isinstance(mv, dict):
                operations.append({"op": "move", "ok": False, "error": "invalid_move_item"})
                continue
            src, src_err = self._validate_workspace_path(str(mv.get("src", "")))
            dst, dst_err = self._validate_workspace_path(str(mv.get("dst", "")))
            if src_err or dst_err:
                operations.append({"op": "move", "src": str(mv.get("src", "")), "dst": str(mv.get("dst", "")), "ok": False, "error": src_err or dst_err})
                continue
            if not src.exists():
                operations.append({"op": "move", "src": str(src), "dst": str(dst), "ok": False, "error": "source_missing"})
                continue
            if apply_changes:
                dst.parent.mkdir(parents=True, exist_ok=True)
                src.rename(dst)
                changed.append(str(dst))
            operations.append({"op": "move", "src": str(src), "dst": str(dst), "ok": True, "applied": apply_changes})

        return {"operations": operations, "changed_files": changed}

    def _execute_work_run(self, run_id: str, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        run_timeout_seconds = self.router.work_run_timeout_seconds()
        run_started_ts = time.time()
        deadline_ts = run_started_ts + run_timeout_seconds
        timeout_stages: Dict[str, Dict[str, int]] = {}
        timed_out_stage = ""

        def _timed_out(stage: str) -> bool:
            nonlocal timed_out_stage
            if time.time() <= deadline_ts:
                return False
            timed_out_stage = stage
            self.run_store.append_event(
                run_id,
                "run_timeout",
                {"stage": stage, "timeout_seconds": run_timeout_seconds},
            )
            return True

        def _remaining_timeout() -> int:
            return max(5, int(deadline_ts - time.time()))

        def _mark_timeout_stage(stage: str) -> None:
            timeout_stages[stage] = {
                "elapsed_ms": int((time.time() - run_started_ts) * 1000),
                "remaining_timeout_seconds": max(0, int(deadline_ts - time.time())),
            }

        def _timeout_trace() -> Dict[str, Any]:
            return {
                "run_timeout_seconds": run_timeout_seconds,
                "elapsed_ms": int((time.time() - run_started_ts) * 1000),
                "timed_out_stage": timed_out_stage,
                "stages": timeout_stages,
            }

        def _timeout_result(message: str, stage: str) -> Tuple[int, Dict[str, Any]]:
            _mark_timeout_stage(stage)
            return 504, {
                "error": {"message": message, "type": "timeout_error"},
                "trace": {"timeout": _timeout_trace()},
            }

        if self.run_store.is_cancel_requested(run_id):
            return 499, {"error": {"message": "Run canceled", "type": "canceled"}}

        mode = str(body.get("mode", "research")).strip().lower() or "research"
        if mode not in {"research", "coding", "organize"}:
            return 400, {"error": {"message": "mode must be one of: research, coding, organize", "type": "invalid_request_error"}}

        project_id = str(body.get("project_id", "default-project")).strip() or "default-project"
        objective = str(body.get("objective", "")).strip()
        if not objective:
            return 400, {"error": {"message": "objective is required", "type": "invalid_request_error"}}
        memory_context = self.memory_store.recall(project_id, limit=8)
        self.run_store.append_event(
            run_id,
            "memory_recalled",
            {
                "facts": len(memory_context.get("facts", [])),
                "decisions": len(memory_context.get("decisions", [])),
                "open_questions": len(memory_context.get("open_questions", [])),
                "next_actions": len(memory_context.get("next_actions", [])),
            },
        )

        constraints: List[str] = []
        raw_constraints = body.get("constraints", [])
        if isinstance(raw_constraints, str) and raw_constraints.strip():
            constraints = [raw_constraints.strip()]
        elif isinstance(raw_constraints, list):
            constraints = [str(x).strip() for x in raw_constraints if str(x).strip()]

        artifacts: List[str] = []
        raw_artifacts = body.get("artifacts", [])
        if isinstance(raw_artifacts, list):
            artifacts = [str(x).strip() for x in raw_artifacts if str(x).strip()]

        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        request_api_key = str(body.get("_request_api_key", "")).strip()
        cognitive_depth, cognitive_depth_error = self._cognitive_depth(body)
        if cognitive_depth_error:
            return 400, {"error": {"message": cognitive_depth_error, "type": "invalid_request_error"}}
        organize_apply = bool(metadata.get("organize_apply", False))
        organize_plan = body.get("organize_plan", {})
        if mode == "organize" and not isinstance(organize_plan, dict):
            organize_plan = {}

        if mode == "organize" and organize_plan:
            if organize_apply:
                auth_ok, auth_err = self._is_privileged_action_allowed("organize_apply", request_api_key)
                if not auth_ok:
                    code = 503 if auth_err == "privileged_auth_misconfigured" else 403
                    return code, {"error": {"message": f"Access denied for organize_apply ({auth_err})", "type": "invalid_request_error"}}
            exec_plan = self._execute_organize_plan(organize_plan, apply_changes=organize_apply)
            approval_expires_at = int(time.time()) + self.router.work_approval_ttl_seconds()
            approval_token, approval_token_id = self._make_approval_token(run_id, approval_expires_at)
            apply_note = "Applied requested organization plan." if organize_apply else "Plan validated only; no filesystem changes applied."
            if not organize_apply:
                self.run_store.update_run(
                    run_id,
                    {
                        "pending_approval": {
                            "token_id": approval_token_id,
                            "organize_plan": organize_plan,
                            "expires_at": approval_expires_at,
                        }
                    },
                )
            return 200, {
                "final_answer": apply_note,
                "next_actions": [
                    "Review operation results and resolve any failed items.",
                    "Apply the validated plan with POST /v1/work/{run_id}/approve and this approval token.",
                ],
                "evidence": [{"organize_plan": organize_plan}],
                "changed_files": exec_plan.get("changed_files", []),
                "memory_updates": [],
                "trace": {"used_plan_apply_path": True},
                "tool_outputs": {"organize_execution": exec_plan},
                "approval": {
                    "required": not organize_apply,
                    "token": approval_token if not organize_apply else "",
                    "expires_at": approval_expires_at if not organize_apply else 0,
                },
            }

        _mark_timeout_stage("before_tool_collection")
        if _timed_out("before_tool_collection"):
            return _timeout_result("Run timed out before model dispatch", "before_tool_collection")

        tools = body.get("tools", {})
        shell_requested = False
        if isinstance(tools, dict):
            shell_cfg = tools.get("shell", {})
            if isinstance(shell_cfg, dict):
                cmd = str(shell_cfg.get("command", "")).strip()
                shell_requested = bool(cmd and bool(metadata.get("allow_shell", False)))
        if shell_requested:
            auth_ok, auth_err = self._is_privileged_action_allowed("shell", request_api_key)
            if not auth_ok:
                code = 503 if auth_err == "privileged_auth_misconfigured" else 403
                return code, {"error": {"message": f"Access denied for shell tool ({auth_err})", "type": "invalid_request_error"}}

        tool_context = self._collect_tool_context(body)
        citations = self._build_citations(tool_context, objective)
        self.run_store.append_event(
            run_id,
            "tool_context_collected",
            {
                "files": len(tool_context.get("files", [])),
                "web": len(tool_context.get("web", [])),
                "web_search": len(tool_context.get("web_search", [])),
                "citations": len(citations),
                "shell": isinstance(tool_context.get("shell"), dict),
            },
        )
        work_prompt = self._build_work_prompt(
            mode=mode,
            project_id=project_id,
            objective=objective,
            constraints=constraints,
            artifacts=artifacts,
            memory=memory_context,
            cognitive_depth=cognitive_depth,
        )
        tool_notes: List[str] = []
        for item in tool_context.get("files", []):
            if isinstance(item, dict):
                p = str(item.get("path", ""))
                c = str(item.get("content", ""))
                if p and c:
                    tool_notes.append(f"[FILE] {p}\n{c}")
        for item in tool_context.get("web", []):
            if isinstance(item, dict):
                u = str(item.get("url", ""))
                s = str(item.get("snippet", ""))
                if u and s:
                    tool_notes.append(f"[WEB] {u}\n{s}")
        for item in tool_context.get("web_search", []):
            if not isinstance(item, dict):
                continue
            q = str(item.get("query", "")).strip()
            hits = item.get("results", [])
            if q and isinstance(hits, list):
                lines = [f"query: {q}"]
                for h in hits[:5]:
                    if isinstance(h, dict):
                        lines.append(f"- {str(h.get('title','')).strip()} | {str(h.get('url','')).strip()}")
                tool_notes.append("[WEB_SEARCH]\n" + "\n".join(lines))
        shell_item = tool_context.get("shell")
        if isinstance(shell_item, dict):
            stdout = str(shell_item.get("stdout", "")).strip()
            if stdout:
                tool_notes.append(f"[SHELL]\n{stdout}")
        if tool_notes:
            joined = "\n\n".join(tool_notes[:8])
            work_prompt = work_prompt + "\n\nTool outputs:\n" + joined[:16000]

        team_default = "off" if cognitive_depth == "low" else ("on" if mode in {"research", "coding"} else "auto")
        verification_default = "strict" if (mode == "research" and cognitive_depth == "deep") else ("strict" if mode == "research" else "standard")
        chat_body: Dict[str, Any] = {
            "model": self.router.gateway_model(),
            "messages": [{"role": "user", "content": work_prompt}],
            "metadata": {
                "router_hint": "reasoning" if mode in {"research", "coding"} else "balanced",
                "team_mode": str(metadata.get("team_mode", team_default)),
                "verification_level": str(metadata.get("verification_level", verification_default)),
            },
        }
        if "provider" in metadata:
            chat_body["metadata"]["provider"] = metadata.get("provider")
        if "team_size" in metadata:
            chat_body["metadata"]["team_size"] = metadata.get("team_size")
        if "max_tool_calls" in metadata:
            chat_body["metadata"]["max_tool_calls"] = metadata.get("max_tool_calls")
        if "tools_autonomy" in metadata:
            chat_body["metadata"]["tools_autonomy"] = metadata.get("tools_autonomy")

        route_name = self.router.pick_route_name(chat_body)
        chat_body, cognitive_policy = self._resolve_cognitive_policy(
            chat_body,
            route_name=route_name,
            explicit_team_endpoint=False,
        )

        if self.run_store.is_cancel_requested(run_id):
            return 499, {"error": {"message": "Run canceled", "type": "canceled"}}
        _mark_timeout_stage("before_dispatch")
        if _timed_out("before_dispatch"):
            return _timeout_result("Run timed out before model dispatch", "before_dispatch")

        provider_pin, provider_error = self._provider_pin(chat_body)
        if provider_error:
            return 400, {"error": {"message": provider_error, "type": "invalid_request_error"}}
        verification_level, verification_error = self._verification_level(chat_body)
        if verification_error:
            return 400, {"error": {"message": verification_error, "type": "invalid_request_error"}}

        route_name = self.router.pick_route_name(chat_body)
        use_team = self._decide_team_mode(chat_body, explicit_team_endpoint=False)
        self.run_store.append_event(
            run_id,
            "dispatch",
            {
                "mode": mode,
                "route": route_name,
                "provider_pin": provider_pin,
                "team": use_team,
                "verification_level": verification_level,
                "cognitive_policy": cognitive_policy,
            },
        )

        if use_team:
            status, payload, headers = self._run_team_orchestration(
                chat_body,
                route_name,
                provider_pin=provider_pin,
                verification_level=verification_level,
                cognitive_policy=cognitive_policy,
            )
        else:
            status, payload, headers = self._run_single_pass(
                chat_body,
                route_name,
                provider_pin=provider_pin,
                timeout_seconds=_remaining_timeout(),
            )
            headers = dict(headers)
            headers["x-router-team"] = "false"
            if status == 200 and isinstance(payload, dict):
                payload["team"] = {
                    "used": False,
                    "route": route_name,
                    "provider_pin": provider_pin,
                    "verification_level": verification_level,
                    "cognitive": cognitive_policy,
                }

        if status != 200:
            return status, payload
        _mark_timeout_stage("after_primary_response")
        if _timed_out("after_primary_response"):
            return _timeout_result("Run timed out after primary response", "after_primary_response")

        final_text = _extract_text_content(payload).strip()
        reflection_trace: Dict[str, Any] = {}
        if cognitive_depth == "deep" and final_text:
            _mark_timeout_stage("before_reflection")
            if _timed_out("before_reflection"):
                return _timeout_result("Run timed out before reflection pass", "before_reflection")
            reflection_prompt = (
                "You are a critical reviewer.\n"
                "Review the draft answer for factual risks, missing checks, and weak assumptions.\n"
                "Return:\n1) Key issues (0-5 bullets)\n2) What to improve before finalizing."
            )
            reflection_body: Dict[str, Any] = {
                "model": self.router.gateway_model(),
                "messages": [
                    {"role": "system", "content": reflection_prompt},
                    {"role": "user", "content": f"Objective:\n{objective}\n\nDraft answer:\n{final_text}"},
                ],
                "metadata": {
                    "router_hint": "reasoning",
                    "team_mode": "off",
                },
            }
            if provider_pin:
                reflection_body["metadata"]["provider"] = provider_pin
            if "max_tool_calls" in metadata:
                reflection_body["metadata"]["max_tool_calls"] = metadata.get("max_tool_calls")
            if "tools_autonomy" in metadata:
                reflection_body["metadata"]["tools_autonomy"] = metadata.get("tools_autonomy")
            self.run_store.append_event(run_id, "reflection_started", {"depth": cognitive_depth})
            r_status, r_payload, r_headers = self._run_single_pass(
                reflection_body,
                route_name,
                provider_pin=provider_pin,
                timeout_seconds=_remaining_timeout(),
            )
            if r_status == 200:
                critique_text = _extract_text_content(r_payload).strip()
                revise_body: Dict[str, Any] = {
                    "model": self.router.gateway_model(),
                    "messages": [
                        {
                            "role": "system",
                            "content": (
                                "You improve drafts using critique.\n"
                                "Produce a corrected final answer with concise verification summary."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Objective:\n{objective}\n\nDraft answer:\n{final_text}\n\nCritique:\n{critique_text}\n\n"
                                "Return a revised final answer and keep actionable next steps."
                            ),
                        },
                    ],
                    "metadata": {"router_hint": "reasoning", "team_mode": "off"},
                }
                if provider_pin:
                    revise_body["metadata"]["provider"] = provider_pin
                if "max_tool_calls" in metadata:
                    revise_body["metadata"]["max_tool_calls"] = metadata.get("max_tool_calls")
                if "tools_autonomy" in metadata:
                    revise_body["metadata"]["tools_autonomy"] = metadata.get("tools_autonomy")
                _mark_timeout_stage("before_reflection_revise")
                if _timed_out("before_reflection_revise"):
                    return _timeout_result("Run timed out before reflection revision", "before_reflection_revise")
                v_status, v_payload, v_headers = self._run_single_pass(
                    revise_body,
                    route_name,
                    provider_pin=provider_pin,
                    timeout_seconds=_remaining_timeout(),
                )
                if v_status == 200:
                    revised_text = _extract_text_content(v_payload).strip()
                    if revised_text:
                        final_text = revised_text
                        headers = dict(v_headers)
                reflection_trace = {
                    "enabled": True,
                    "critique_backend": r_headers.get("x-router-backend", ""),
                    "revised": v_status == 200,
                }
            else:
                reflection_trace = {"enabled": True, "critique_failed": True, "status": r_status}

        next_actions = self._extract_next_actions(final_text)
        memory_updates = self._extract_memory_updates(final_text)
        open_questions = self._extract_open_questions(final_text)
        result = {
            "final_answer": final_text,
            "next_actions": next_actions,
            "evidence": [
                {
                    "route": route_name,
                    "provider": headers.get("x-router-provider", ""),
                    "backend": headers.get("x-router-backend", ""),
                    "team_used": headers.get("x-router-team", "false") == "true",
                    "cognitive_depth": cognitive_depth,
                }
            ],
            "changed_files": [],
            "memory_updates": memory_updates[:6] if memory_updates else ([f"{project_id}: {next_actions[0]}"] if next_actions else []),
            "open_questions": open_questions[:3],
            "trace": {
                "team": payload.get("team", {}),
                "tool_loop": payload.get("tool_loop", {}),
                "cognitive": cognitive_policy,
                "reflection": reflection_trace,
                "timeout": _timeout_trace(),
            },
            "tool_outputs": tool_context,
        }
        result["memory_context_used"] = memory_context
        if artifacts:
            result["evidence"].append({"provided_artifacts": artifacts})
        if citations:
            result["evidence"].append({"citations": citations})
        if mode == "organize":
            result["next_actions"].append("If you want filesystem changes, provide organize_plan and set metadata.organize_apply=true.")
        return 200, result

    def _execute_and_finalize_run(self, run_id: str, body: Dict[str, Any]) -> None:
        self.run_store.update_run(run_id, {"status": "running", "updated_at": int(time.time())})
        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        provider = str(metadata.get("provider", "")).strip().lower()
        retry_cfg = self.router.work_provider_retry(provider) if provider else {
            "retry_count": self.router.work_retry_count(),
            "retry_backoff_seconds": self.router.work_retry_backoff_seconds(),
        }
        max_retries = int(retry_cfg.get("retry_count", 0))
        backoff = int(retry_cfg.get("retry_backoff_seconds", 2))
        attempt = 0
        status = 500
        result: Dict[str, Any] = {"error": {"message": "Unknown execution error", "type": "server_error"}}
        while attempt <= max_retries:
            attempt += 1
            self.run_store.append_event(
                run_id,
                "attempt_started",
                {"attempt": attempt, "max_retries": max_retries, "provider": provider or "auto"},
            )
            status, result = self._execute_work_run(run_id, body)
            if status == 200:
                break
            if status == 499:
                break
            err = result.get("error", {}) if isinstance(result, dict) else {}
            err_type = str(err.get("type", "")).strip().lower() if isinstance(err, dict) else ""
            should_retry = (status in {500, 502, 503, 504} or err_type in {"upstream_error", "server_error"}) and err_type != "timeout_error"
            if not should_retry or attempt > max_retries:
                break
            self.run_store.append_event(
                run_id,
                "retry_scheduled",
                {"attempt": attempt, "next_attempt": attempt + 1, "backoff_seconds": backoff, "status": status},
            )
            time.sleep(backoff)

        if status == 499 or self.run_store.is_cancel_requested(run_id):
            self.run_store.update_run(run_id, {"status": "canceled", "updated_at": int(time.time())})
            self.run_store.append_event(run_id, "canceled", {"reason": "user_requested"})
            return
        if status == 200:
            project_id = str(body.get("project_id", "default-project")).strip() or "default-project"
            objective = str(body.get("objective", "")).strip()
            memory_updates_raw = result.get("memory_updates", []) if isinstance(result, dict) else []
            next_actions_raw = result.get("next_actions", []) if isinstance(result, dict) else []
            open_questions_raw = result.get("open_questions", []) if isinstance(result, dict) else []
            memory_updates = [str(x).strip() for x in memory_updates_raw if str(x).strip()] if isinstance(memory_updates_raw, list) else []
            next_actions = [str(x).strip() for x in next_actions_raw if str(x).strip()] if isinstance(next_actions_raw, list) else []
            open_questions = [str(x).strip() for x in open_questions_raw if str(x).strip()] if isinstance(open_questions_raw, list) else []
            self.memory_store.append_updates(
                project_id=project_id,
                objective=objective,
                memory_updates=memory_updates,
                next_actions=next_actions,
                open_questions=open_questions,
            )
            self.run_store.update_run(run_id, {"status": "done", "updated_at": int(time.time()), "result": result, "error_status": None})
            self.run_store.append_event(run_id, "completed", {"ok": True})
        else:
            self.run_store.update_run(
                run_id,
                {"status": "failed", "updated_at": int(time.time()), "error": result.get("error"), "error_status": status},
            )
            self.run_store.append_event(run_id, "failed", {"status": status, "error": result.get("error")})

    def _send_html(self, status: int, html: str) -> None:
        req_id = self._ensure_request_id()
        payload = html.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("x-request-id", req_id)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "authorization,content-type")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.end_headers()
        self.wfile.write(payload)
        self.event_logger.log(
            "http_response",
            {
                "request_id": req_id,
                "method": str(getattr(self, "command", "")),
                "path": str(getattr(self, "_route_path", "")),
                "status": int(status),
                "response_bytes": len(payload),
                "content_type": "text/html",
            },
        )

    def _send_text(self, status: int, text: str, content_type: str, filename: str = "") -> None:
        req_id = self._ensure_request_id()
        payload = text.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("x-request-id", req_id)
        if filename:
            self.send_header("Content-Disposition", f'attachment; filename="{filename}"')
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "authorization,content-type")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.end_headers()
        self.wfile.write(payload)
        self.event_logger.log(
            "http_response",
            {
                "request_id": req_id,
                "method": str(getattr(self, "command", "")),
                "path": str(getattr(self, "_route_path", "")),
                "status": int(status),
                "response_bytes": len(payload),
                "content_type": content_type,
                "filename": filename,
            },
        )

    def _admin_index_path(self) -> Path:
        return project_root() / "admin" / "index.html"

    def _admin_provider_rows(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        backends = config.get("backends", {})
        routes = config.get("routing", {}).get("routes", []) if isinstance(config.get("routing", {}), dict) else []
        route_map: Dict[str, List[str]] = {}
        provider_health = self.state.provider_health_snapshot()
        if isinstance(routes, list):
            for route in routes:
                if not isinstance(route, dict):
                    continue
                route_name = str(route.get("name", "")).strip()
                if not route_name:
                    continue
                order = route.get("order", [])
                if not isinstance(order, list):
                    continue
                for item in order:
                    if not isinstance(item, dict):
                        continue
                    if str(item.get("type", "")).strip().lower() != "openai":
                        continue
                    backend_name = str(item.get("backend", "")).strip()
                    if not backend_name:
                        continue
                    bucket = route_map.setdefault(backend_name, [])
                    if route_name not in bucket:
                        bucket.append(route_name)
        out: List[Dict[str, Any]] = []
        if isinstance(backends, dict):
            for name, cfg in backends.items():
                if not isinstance(cfg, dict):
                    continue
                provider = str(cfg.get("provider", "")).strip().lower() or _infer_provider_from_name(str(name))
                provider_item = provider_health.get(provider, {})
                out.append(
                    {
                        "name": str(name),
                        "kind": str(cfg.get("kind", "openai")).strip().lower() or "openai",
                        "provider": provider,
                        "enabled": bool(cfg.get("enabled", True)),
                        "base_url": str(cfg.get("base_url", "")).strip(),
                        "model": str(cfg.get("model", "")).strip(),
                        "api_key_env": str(cfg.get("api_key_env", "")).strip(),
                        "api_key_file": str(cfg.get("api_key_file", "")).strip(),
                        "extra_headers": cfg.get("extra_headers", {}) if isinstance(cfg.get("extra_headers", {}), dict) else {},
                        "route_names": sorted(route_map.get(str(name), [])),
                        "health": {
                            "last_success_epoch": int(provider_item.get("last_success_epoch", 0)),
                            "circuit_open": bool(provider_item.get("circuit_open", False)),
                            "open_until_epoch": int(provider_item.get("open_until_epoch", 0)),
                            "failures": int(provider_item.get("failures", 0)),
                        },
                    }
                )
        out.sort(key=lambda row: row.get("name", ""))
        return out

    def _admin_local_model_rows(self, registry: Dict[str, Any], status_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        cluster = registry.get("lm_studio_cluster", {})
        nodes = cluster.get("nodes", []) if isinstance(cluster, dict) else []
        loaded_map: Dict[str, set[str]] = {}
        lmstudio = status_payload.get("lmstudio", [])
        if isinstance(lmstudio, list):
            for item in lmstudio:
                if not isinstance(item, dict):
                    continue
                base_url = str(item.get("base_url", "")).strip().rstrip("/")
                if not base_url:
                    continue
                loaded = item.get("loaded_models", [])
                loaded_map[base_url] = {
                    str(mid).strip()
                    for mid in loaded
                    if str(mid).strip()
                }
        rows: List[Dict[str, Any]] = []
        if not isinstance(nodes, list):
            return rows
        for node in nodes:
            if not isinstance(node, dict):
                continue
            base_url = str(node.get("base_url", "")).strip().rstrip("/")
            if not base_url:
                continue
            models = node.get("models", [])
            if not isinstance(models, list):
                continue
            loaded_models = loaded_map.get(base_url, set())
            for model in models:
                if not isinstance(model, dict):
                    continue
                model_id = str(model.get("id", "")).strip()
                if not model_id:
                    continue
                row = {
                    "base_url": base_url,
                    "id": model_id,
                    "role": str(model.get("role", "")).strip(),
                    "enabled": bool(model.get("enabled", True)),
                    "loaded": model_id in loaded_models,
                }
                if "size_gb" in model:
                    row["size_gb"] = model.get("size_gb")
                if "parallel" in model:
                    row["parallel"] = model.get("parallel")
                rows.append(row)
        rows.sort(key=lambda r: (str(r.get("base_url", "")), str(r.get("id", ""))))
        return rows

    def _admin_bootstrap_payload(self) -> Dict[str, Any]:
        config = self.router.config_snapshot()
        self.state.probe_if_due(force=False)
        status = self.state.status_payload()
        local_registry = self.router._load_local_registry()
        return {
            "ok": True,
            "gateway_model": self.router.gateway_model(),
            "config_path": str(self.router.config_path),
            "local_registry_path": str(self.router.local_registry_path()),
            "config": _redacted_config_for_admin(config),
            "providers": self._admin_provider_rows(config),
            "team_stage_roles": self.router.team_stage_roles(),
            "local_models": self._admin_local_model_rows(local_registry, status),
            "runs": self.run_store.list_runs(limit=80),
            "status": status,
            "logging": {
                "enabled": self.router.logging_enabled(),
                "path": str(self.router.logging_path()),
            },
            "now": int(time.time()),
        }

    def _admin_logs_payload(
        self,
        limit: int,
        event_type: str = "",
        level: str = "",
        request_id: str = "",
        run_id: str = "",
    ) -> Dict[str, Any]:
        events = self.event_logger.read_events(
            limit=limit,
            event_type=event_type,
            level=level,
            request_id=request_id,
            run_id=run_id,
        )
        return {
            "ok": True,
            "events": events,
            "count": len(events),
            "filters": {
                "limit": limit,
                "event_type": event_type,
                "level": level,
                "request_id": request_id,
                "run_id": run_id,
            },
        }

    def _admin_upsert_local_model(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        base_url = str(body.get("base_url", "")).strip().rstrip("/")
        model_id = str(body.get("id", "")).strip()
        role = str(body.get("role", "")).strip()
        enabled = bool(body.get("enabled", True))
        if not base_url.startswith("http://") and not base_url.startswith("https://"):
            return 400, {"error": {"message": "base_url must be http(s) URL", "type": "invalid_request_error"}}
        if not model_id:
            return 400, {"error": {"message": "id is required", "type": "invalid_request_error"}}
        if not role:
            return 400, {"error": {"message": "role is required", "type": "invalid_request_error"}}
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{2,60}", role):
            return 400, {"error": {"message": "role has invalid format", "type": "invalid_request_error"}}

        registry = self.router._load_local_registry()
        cluster = registry.get("lm_studio_cluster", {})
        if not isinstance(cluster, dict):
            cluster = {}
        nodes = cluster.get("nodes", [])
        if not isinstance(nodes, list):
            nodes = []
        target_node: Optional[Dict[str, Any]] = None
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_base = str(node.get("base_url", "")).strip().rstrip("/")
            if node_base == base_url:
                target_node = node
                break
        if target_node is None:
            target_node = {"base_url": base_url, "models": []}
            nodes.append(target_node)
        models = target_node.get("models", [])
        if not isinstance(models, list):
            models = []
            target_node["models"] = models
        target_model: Optional[Dict[str, Any]] = None
        for model in models:
            if not isinstance(model, dict):
                continue
            if str(model.get("id", "")).strip() == model_id:
                target_model = model
                break
        if target_model is None:
            target_model = {"id": model_id}
            models.append(target_model)
        target_model["id"] = model_id
        target_model["role"] = role
        target_model["enabled"] = enabled

        if "size_gb" in body and str(body.get("size_gb", "")).strip():
            try:
                target_model["size_gb"] = float(body.get("size_gb"))
            except Exception:
                return 400, {"error": {"message": "size_gb must be numeric", "type": "invalid_request_error"}}
        if "parallel" in body and str(body.get("parallel", "")).strip():
            try:
                parallel = int(body.get("parallel"))
                if parallel <= 0:
                    raise ValueError("parallel must be > 0")
                target_model["parallel"] = parallel
            except Exception:
                return 400, {"error": {"message": "parallel must be a positive integer", "type": "invalid_request_error"}}

        target_node["models"] = models
        cluster["nodes"] = nodes
        registry["lm_studio_cluster"] = cluster
        self.router.save_local_registry(registry)
        self.state.reset()
        self.state.probe_if_due(force=False)
        return 200, {"ok": True}

    def _admin_set_local_model_enabled(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        base_url = str(body.get("base_url", "")).strip().rstrip("/")
        model_id = str(body.get("id", "")).strip()
        enabled = bool(body.get("enabled", True))
        if not base_url or not model_id:
            return 400, {"error": {"message": "base_url and id are required", "type": "invalid_request_error"}}

        registry = self.router._load_local_registry()
        cluster = registry.get("lm_studio_cluster", {})
        if not isinstance(cluster, dict):
            return 404, {"error": {"message": "local registry missing lm_studio_cluster", "type": "invalid_request_error"}}
        nodes = cluster.get("nodes", [])
        if not isinstance(nodes, list):
            return 404, {"error": {"message": "local registry missing nodes", "type": "invalid_request_error"}}

        found = False
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_base = str(node.get("base_url", "")).strip().rstrip("/")
            if node_base != base_url:
                continue
            models = node.get("models", [])
            if not isinstance(models, list):
                continue
            for model in models:
                if not isinstance(model, dict):
                    continue
                if str(model.get("id", "")).strip() == model_id:
                    model["enabled"] = enabled
                    found = True
                    break
            if found:
                break
        if not found:
            return 404, {"error": {"message": "Local model mapping not found", "type": "invalid_request_error"}}

        cluster["nodes"] = nodes
        registry["lm_studio_cluster"] = cluster
        self.router.save_local_registry(registry)
        self.state.reset()
        self.state.probe_if_due(force=False)
        return 200, {"ok": True, "enabled": enabled}

    def _admin_delete_local_model(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        base_url = str(body.get("base_url", "")).strip().rstrip("/")
        model_id = str(body.get("id", "")).strip()
        if not base_url or not model_id:
            return 400, {"error": {"message": "base_url and id are required", "type": "invalid_request_error"}}

        registry = self.router._load_local_registry()
        cluster = registry.get("lm_studio_cluster", {})
        if not isinstance(cluster, dict):
            return 404, {"error": {"message": "local registry missing lm_studio_cluster", "type": "invalid_request_error"}}
        nodes = cluster.get("nodes", [])
        if not isinstance(nodes, list):
            return 404, {"error": {"message": "local registry missing nodes", "type": "invalid_request_error"}}

        found = False
        kept_nodes: List[Dict[str, Any]] = []
        for node in nodes:
            if not isinstance(node, dict):
                continue
            node_base = str(node.get("base_url", "")).strip().rstrip("/")
            models = node.get("models", [])
            if node_base != base_url or not isinstance(models, list):
                kept_nodes.append(node)
                continue
            filtered: List[Dict[str, Any]] = []
            for model in models:
                if not isinstance(model, dict):
                    continue
                if str(model.get("id", "")).strip() == model_id:
                    found = True
                    continue
                filtered.append(model)
            if filtered:
                node["models"] = filtered
                kept_nodes.append(node)
        if not found:
            return 404, {"error": {"message": "Local model mapping not found", "type": "invalid_request_error"}}

        cluster["nodes"] = kept_nodes
        registry["lm_studio_cluster"] = cluster
        self.router.save_local_registry(registry)
        self.state.reset()
        self.state.probe_if_due(force=False)
        return 200, {"ok": True}

    def _admin_upsert_provider(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        name = str(body.get("name", "")).strip()
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{2,80}", name):
            return 400, {"error": {"message": "Invalid provider backend name", "type": "invalid_request_error"}}
        kind = str(body.get("kind", "openai")).strip().lower() or "openai"
        if kind != "openai":
            return 400, {"error": {"message": "Only kind=openai is supported for admin providers", "type": "invalid_request_error"}}
        base_url = str(body.get("base_url", "")).strip().rstrip("/")
        if base_url.endswith("/chat/completions"):
            base_url = base_url[: -len("/chat/completions")].rstrip("/")
        if not (base_url.startswith("https://") or base_url.startswith("http://")):
            return 400, {"error": {"message": "base_url must start with http:// or https:// and should usually end with /v1", "type": "invalid_request_error"}}
        model = str(body.get("model", "")).strip()
        if not base_url or not model:
            return 400, {"error": {"message": "base_url and model are required", "type": "invalid_request_error"}}
        provider = str(body.get("provider", "")).strip().lower() or _infer_provider_from_name(name)
        api_key_env = str(body.get("api_key_env", "")).strip()
        api_key_file = str(body.get("api_key_file", "")).strip()
        api_key_value = str(body.get("api_key_value", "")).strip()
        enabled_raw = body.get("enabled", None)
        route_names_raw = body.get("route_names", [])
        route_names = [str(x).strip() for x in route_names_raw if str(x).strip()] if isinstance(route_names_raw, list) else []
        extra_headers_raw = body.get("extra_headers", {})
        extra_headers: Dict[str, str] = {}
        if isinstance(extra_headers_raw, dict):
            for key, value in extra_headers_raw.items():
                k = str(key).strip()
                v = str(value).strip()
                if k and v:
                    extra_headers[k] = v

        config = self.router.config_snapshot()
        backends = config.get("backends", {})
        if not isinstance(backends, dict):
            backends = {}
        existing = backends.get(name, {})
        existing_enabled = bool(existing.get("enabled", True)) if isinstance(existing, dict) else True

        enabled = existing_enabled
        if isinstance(enabled_raw, bool):
            enabled = enabled_raw
        elif isinstance(enabled_raw, str):
            low = enabled_raw.strip().lower()
            if low in {"true", "1", "yes", "on"}:
                enabled = True
            elif low in {"false", "0", "no", "off"}:
                enabled = False

        if api_key_value:
            if not api_key_file:
                safe_name = re.sub(r"[^a-zA-Z0-9_.-]+", "_", name)
                api_key_file = f"config/secrets/{safe_name}.key"
            key_path = _abs_path(api_key_file)
            key_path.parent.mkdir(parents=True, exist_ok=True)
            key_path.write_text(api_key_value + "\n", encoding="utf-8")
            try:
                os.chmod(key_path, 0o600)
            except Exception:
                pass

        backend_cfg: Dict[str, Any] = {
            "kind": kind,
            "provider": provider,
            "enabled": enabled,
            "base_url": base_url,
            "model": model,
        }
        if api_key_env:
            backend_cfg["api_key_env"] = api_key_env
        if api_key_file:
            backend_cfg["api_key_file"] = api_key_file
        if extra_headers:
            backend_cfg["extra_headers"] = extra_headers
        backends[name] = backend_cfg
        config["backends"] = backends

        routing = config.get("routing", {})
        routes = routing.get("routes", []) if isinstance(routing, dict) else []
        if isinstance(routes, list):
            wanted = set(route_names)
            for route in routes:
                if not isinstance(route, dict):
                    continue
                route_name = str(route.get("name", "")).strip()
                if not route_name:
                    continue
                order = route.get("order", [])
                if not isinstance(order, list):
                    order = []
                has_entry = any(
                    isinstance(item, dict)
                    and str(item.get("type", "")).strip().lower() == "openai"
                    and str(item.get("backend", "")).strip() == name
                    for item in order
                )
                if route_name in wanted and not has_entry:
                    order.append({"type": "openai", "backend": name})
                    route["order"] = order

        self.router.save_config(config)
        self.state.reset()
        self.state.probe_if_due(force=False)
        return 200, {"ok": True, "provider": name, "providers": self._admin_provider_rows(config)}

    def _admin_set_provider_enabled(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        name = str(body.get("name", "")).strip()
        if not name:
            return 400, {"error": {"message": "Provider name is required", "type": "invalid_request_error"}}

        enabled_raw = body.get("enabled", True)
        enabled = bool(enabled_raw)
        if isinstance(enabled_raw, str):
            low = enabled_raw.strip().lower()
            if low in {"true", "1", "yes", "on"}:
                enabled = True
            elif low in {"false", "0", "no", "off"}:
                enabled = False

        config = self.router.config_snapshot()
        backends = config.get("backends", {})
        if not isinstance(backends, dict):
            return 404, {"error": {"message": "backends config missing", "type": "invalid_request_error"}}
        cfg = backends.get(name, {})
        if not isinstance(cfg, dict):
            return 404, {"error": {"message": f"Provider backend not found: {name}", "type": "invalid_request_error"}}
        cfg["enabled"] = enabled
        backends[name] = cfg
        config["backends"] = backends
        self.router.save_config(config)
        self.state.reset()
        self.state.probe_if_due(force=False)
        return 200, {"ok": True, "provider": name, "enabled": enabled, "providers": self._admin_provider_rows(config)}

    def _admin_update_team_stage_roles(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        raw = body.get("stage_roles", {})
        if not isinstance(raw, dict):
            return 400, {"error": {"message": "stage_roles must be an object", "type": "invalid_request_error"}}

        allowed = {"planner", "solver", "verifier", "strict_checker", "synthesizer"}
        next_roles: Dict[str, str] = {}
        for key in allowed:
            value = str(raw.get(key, "auto")).strip() or "auto"
            low = value.lower()
            if low == "auto":
                next_roles[key] = "auto"
                continue
            if re.fullmatch(r"(provider|backend|local_role):[A-Za-z0-9_.:-]{1,120}", value):
                next_roles[key] = value
                continue
            return 400, {
                "error": {
                    "message": (
                        f"Invalid stage role for {key}: use auto or "
                        "provider:<family> / backend:<backend_name> / local_role:<role>"
                    ),
                    "type": "invalid_request_error",
                }
            }

        config = self.router.config_snapshot()
        team = config.get("team_orchestration", {})
        if not isinstance(team, dict):
            team = {}
        team["stage_roles"] = next_roles
        config["team_orchestration"] = team
        self.router.save_config(config)
        self.state.reset()
        self.state.probe_if_due(force=False)
        return 200, {"ok": True, "stage_roles": next_roles}

    def _admin_update_cognitive_routing(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        raw = body.get("cognitive_routing", body)
        if not isinstance(raw, dict):
            return 400, {"error": {"message": "cognitive_routing must be an object", "type": "invalid_request_error"}}

        enabled_raw = raw.get("enabled", True)
        enabled = bool(enabled_raw)
        if isinstance(enabled_raw, str):
            enabled = enabled_raw.strip().lower() in {"true", "1", "yes", "on"}

        thresholds_raw = raw.get("difficulty_thresholds", {})
        if not isinstance(thresholds_raw, dict):
            thresholds_raw = {}
        try:
            plan = float(thresholds_raw.get("plan_solve", 0.35))
            debate = float(thresholds_raw.get("debate_verify", 0.62))
            search = float(thresholds_raw.get("search_reflect", 0.80))
        except Exception:
            return 400, {"error": {"message": "difficulty_thresholds values must be numbers", "type": "invalid_request_error"}}
        plan = max(0.05, min(0.95, plan))
        debate = max(plan, min(0.98, debate))
        search = max(debate, min(0.99, search))
        thresholds = {"plan_solve": plan, "debate_verify": debate, "search_reflect": search}

        allowed_modes = ["fast_path", "plan_solve", "debate_verify", "search_reflect"]
        existing_profiles = self.router.cognitive_budget_profiles()
        profiles_raw = raw.get("budget_profiles", {})
        if not isinstance(profiles_raw, dict):
            profiles_raw = {}
        merged_profiles: Dict[str, Dict[str, Any]] = {}
        for mode in allowed_modes:
            base = dict(existing_profiles.get(mode, {}))
            cand = profiles_raw.get(mode, {})
            if not isinstance(cand, dict):
                cand = {}
            merged = dict(base)
            try:
                if "team_size" in cand:
                    merged["team_size"] = max(2, min(6, int(cand.get("team_size", merged.get("team_size", 3)))))
                if "verification_level" in cand:
                    level = str(cand.get("verification_level", merged.get("verification_level", "standard"))).strip().lower()
                    if level not in {"light", "standard", "strict"}:
                        return 400, {"error": {"message": f"Invalid verification_level for {mode}", "type": "invalid_request_error"}}
                    merged["verification_level"] = level
                if "max_tool_calls" in cand:
                    merged["max_tool_calls"] = max(0, min(20, int(cand.get("max_tool_calls", merged.get("max_tool_calls", 2)))))
                if "step_timeout_seconds" in cand:
                    merged["step_timeout_seconds"] = max(5, min(180, int(cand.get("step_timeout_seconds", merged.get("step_timeout_seconds", 45)))))
            except Exception:
                return 400, {"error": {"message": f"Invalid numeric profile fields for {mode}", "type": "invalid_request_error"}}
            merged_profiles[mode] = merged

        config = self.router.config_snapshot()
        config["cognitive_routing"] = {
            "enabled": enabled,
            "difficulty_thresholds": thresholds,
            "budget_profiles": merged_profiles,
        }
        self.router.save_config(config)
        self.state.reset()
        self.state.probe_if_due(force=False)
        return 200, {"ok": True, "cognitive_routing": config["cognitive_routing"]}

    def _admin_remove_provider(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        name = str(body.get("name", "")).strip()
        if not name:
            return 400, {"error": {"message": "Provider name is required", "type": "invalid_request_error"}}
        config = self.router.config_snapshot()
        backends = config.get("backends", {})
        if not isinstance(backends, dict) or name not in backends:
            return 404, {"error": {"message": f"Provider backend not found: {name}", "type": "invalid_request_error"}}
        backends.pop(name, None)
        config["backends"] = backends
        routing = config.get("routing", {})
        routes = routing.get("routes", []) if isinstance(routing, dict) else []
        if isinstance(routes, list):
            for route in routes:
                if not isinstance(route, dict):
                    continue
                order = route.get("order", [])
                if not isinstance(order, list):
                    continue
                route["order"] = [
                    item
                    for item in order
                    if not (
                        isinstance(item, dict)
                        and str(item.get("type", "")).strip().lower() == "openai"
                        and str(item.get("backend", "")).strip() == name
                    )
                ]
        self.router.save_config(config)
        self.state.reset()
        self.state.probe_if_due(force=False)
        return 200, {"ok": True, "removed": name, "providers": self._admin_provider_rows(config)}

    def _admin_test_provider(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        name = str(body.get("name", "")).strip()
        if not name:
            return 400, {"error": {"message": "Provider name is required", "type": "invalid_request_error"}}
        config = self.router.config_snapshot()
        backends = config.get("backends", {})
        if not isinstance(backends, dict):
            backends = {}
        cfg = backends.get(name, {})
        if not isinstance(cfg, dict):
            return 404, {"error": {"message": f"Provider backend not found: {name}", "type": "invalid_request_error"}}
        backend_name = str(name).strip()
        try:
            backend = self.router._named_openai_backend(backend_name)
        except Exception as exc:
            return 400, {"error": {"message": f"Invalid backend config: {exc}", "type": "invalid_request_error"}}
        started = time.time()
        try:
            resp = call_backend(
                backend,
                {
                    "messages": [{"role": "user", "content": "Reply with exactly: ok"}],
                    "max_tokens": 12,
                    "temperature": 0,
                },
                timeout_seconds=min(30, self.router.request_timeout_seconds()),
            )
            text = _extract_text_content(resp).strip()
            return 200, {"ok": True, "provider": name, "elapsed_ms": int((time.time() - started) * 1000), "preview": text[:120]}
        except Exception as exc:
            return 502, {
                "ok": False,
                "provider": name,
                "elapsed_ms": int((time.time() - started) * 1000),
                "error": str(exc)[:300],
            }

    def _admin_chat_preview(self, body: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
        prompt = str(body.get("prompt", "")).strip()
        if not prompt:
            return 400, {"error": {"message": "prompt is required", "type": "invalid_request_error"}}
        provider = str(body.get("provider", "")).strip().lower()
        team_mode = str(body.get("team_mode", "off")).strip().lower() or "off"
        router_hint = str(body.get("router_hint", "reasoning")).strip() or "reasoning"
        verification_level = str(body.get("verification_level", "standard")).strip().lower() or "standard"
        req: Dict[str, Any] = {
            "model": self.router.gateway_model(),
            "messages": [{"role": "user", "content": prompt}],
            "metadata": {"team_mode": team_mode, "router_hint": router_hint, "verification_level": verification_level},
        }
        if provider:
            req["metadata"]["provider"] = provider

        validation_error = self._validate_chat_request(req)
        if validation_error:
            return 400, {"error": {"message": validation_error, "type": "invalid_request_error"}}

        route_name = self.router.pick_route_name(req)
        req, cognitive_policy = self._resolve_cognitive_policy(req, route_name=route_name, explicit_team_endpoint=False)
        provider_pin, provider_error = self._provider_pin(req)
        if provider_error:
            return 400, {"error": {"message": provider_error, "type": "invalid_request_error"}}
        verification_level, verification_error = self._verification_level(req)
        if verification_error:
            return 400, {"error": {"message": verification_error, "type": "invalid_request_error"}}

        use_team = str(req.get("metadata", {}).get("team_mode", team_mode)).strip().lower() == "on"
        if use_team:
            status, payload, headers = self._run_team_orchestration(
                req,
                route_name,
                provider_pin=provider_pin,
                verification_level=verification_level,
                cognitive_policy=cognitive_policy,
            )
        else:
            status, payload, headers = self._run_single_pass(req, route_name, provider_pin=provider_pin)
            headers = dict(headers)
            headers["x-router-team"] = "false"
            if status == 200 and isinstance(payload, dict):
                payload["team"] = {
                    "used": False,
                    "route": route_name,
                    "provider_pin": provider_pin,
                    "verification_level": verification_level,
                    "cognitive": cognitive_policy,
                }

        assistant = _extract_text_content(payload) if isinstance(payload, dict) else ""
        return status, {
            "status": status,
            "assistant": assistant,
            "raw": payload,
            "router_headers": headers,
            "route": route_name,
            "team_mode": team_mode,
            "provider_pin": provider_pin,
            "cognitive_policy": cognitive_policy,
        }

    def _match_stage_preference(self, backend: Backend, preference: str) -> bool:
        pref = str(preference).strip()
        if not pref or pref.lower() == "auto":
            return False
        if ":" not in pref:
            return False
        pref_type, pref_value = pref.split(":", 1)
        pref_type = pref_type.strip().lower()
        pref_value = pref_value.strip()
        if not pref_value:
            return False
        if pref_type == "provider":
            return backend.provider == pref_value.lower()
        if pref_type == "local_role":
            return backend.kind == "local" and backend.source_role.lower() == pref_value.lower()
        if pref_type == "backend":
            return backend.name == pref_value or backend.name.startswith(f"openai:{pref_value}:")
        return False

    def _pick_stage_backend(self, ordered: List[Backend], preference: str, fallback: Backend) -> Backend:
        for backend in ordered:
            if self._match_stage_preference(backend, preference):
                return backend
        return fallback

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_json(200, {"ok": True})

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        route_path = parsed.path
        if route_path != "/":
            route_path = route_path.rstrip("/")
        self._route_path = route_path
        self.event_logger.log(
            "http_request",
            {
                "request_id": self._ensure_request_id(),
                "method": "GET",
                "path": route_path,
                "query": parsed.query[:1000],
            },
        )
        query = parse_qs(parsed.query)
        if route_path == "/":
            self.send_response(302)
            self.send_header("Location", "/-/admin")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            return
        allowed, denial = self._check_admin_access(route_path)
        if not allowed:
            code, payload, headers = denial if denial is not None else (403, {"error": {"message": "Forbidden"}}, {})
            self._send_json(code, payload, extra_headers=headers)
            return

        if route_path in {"/-/admin", "/admin"}:
            index_path = self._admin_index_path()
            if not index_path.exists():
                self._send_json(404, {"error": {"message": "Admin UI not found", "type": "invalid_request_error"}})
                return
            self._send_html(200, index_path.read_text(encoding="utf-8"))
            return
        if route_path == "/-/admin/api/bootstrap":
            self._send_json(200, self._admin_bootstrap_payload())
            return
        if route_path == "/-/admin/api/runs":
            limit = 80
            try:
                limit = int((query.get("limit", ["80"])[0] or "80").strip())
            except Exception:
                limit = 80
            self._send_json(200, {"ok": True, "runs": self.run_store.list_runs(limit=limit)})
            return
        if route_path == "/-/admin/api/logs":
            limit = 200
            try:
                limit = int((query.get("limit", ["200"])[0] or "200").strip())
            except Exception:
                limit = 200
            event_type = str((query.get("event_type", [""])[0] or "").strip())
            level = str((query.get("level", [""])[0] or "").strip())
            request_id = str((query.get("request_id", [""])[0] or "").strip())
            run_id = str((query.get("run_id", [""])[0] or "").strip())
            self._send_json(200, self._admin_logs_payload(limit, event_type=event_type, level=level, request_id=request_id, run_id=run_id))
            return
        if route_path == "/-/admin/api/logs/export":
            limit = 200
            try:
                limit = int((query.get("limit", ["200"])[0] or "200").strip())
            except Exception:
                limit = 200
            event_type = str((query.get("event_type", [""])[0] or "").strip())
            level = str((query.get("level", [""])[0] or "").strip())
            request_id = str((query.get("request_id", [""])[0] or "").strip())
            run_id = str((query.get("run_id", [""])[0] or "").strip())
            fmt = str((query.get("format", ["jsonl"])[0] or "jsonl").strip().lower())
            if fmt not in {"jsonl", "json", "csv"}:
                self._send_json(400, {"error": {"message": "format must be one of: jsonl, json, csv", "type": "invalid_request_error"}})
                return
            text, content_type = self.event_logger.export_events_text(
                fmt=fmt,
                limit=limit,
                event_type=event_type,
                level=level,
                request_id=request_id,
                run_id=run_id,
            )
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            filename = f"gateway_logs_{stamp}.{fmt}"
            self._send_text(200, text, content_type=content_type, filename=filename)
            return

        is_work_route, run_id, subroute = self._work_route(route_path)
        if is_work_route and run_id:
            if subroute == "cancel":
                self._send_json(405, {"error": {"message": "Use POST for cancel", "type": "invalid_request_error"}})
                return
            run = self.run_store.get_run(run_id)
            if not isinstance(run, dict):
                self._send_json(404, {"error": {"message": f"Run not found: {run_id}", "type": "invalid_request_error"}})
                return
            if subroute == "events":
                self._send_json(200, {"id": run_id, "events": self.run_store.get_events(run_id)})
                return
            if subroute == "events_stream":
                since = 0
                wait_seconds = 20
                try:
                    since = int((query.get("since", ["0"])[0] or "0").strip())
                except Exception:
                    since = 0
                try:
                    wait_seconds = max(1, min(60, int((query.get("wait_seconds", ["20"])[0] or "20").strip())))
                except Exception:
                    wait_seconds = 20
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream; charset=utf-8")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "close")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                start = time.time()
                sent = 0
                while time.time() - start < wait_seconds:
                    events = self.run_store.get_events(run_id)
                    new_items = events[since + sent :]
                    if new_items:
                        for event in new_items:
                            payload = json.dumps(event, ensure_ascii=False)
                            self.wfile.write(f"event: {event.get('type','message')}\n".encode("utf-8"))
                            self.wfile.write(f"data: {payload}\n\n".encode("utf-8"))
                            self.wfile.flush()
                            sent += 1
                        run_state = self.run_store.get_run(run_id) or {}
                        status = str(run_state.get("status", "")).strip().lower()
                        if status in {"done", "failed", "canceled"}:
                            break
                    else:
                        run_state = self.run_store.get_run(run_id) or {}
                        status = str(run_state.get("status", "")).strip().lower()
                        if status in {"done", "failed", "canceled"}:
                            break
                        self.wfile.write(b": keep-alive\n\n")
                        self.wfile.flush()
                        time.sleep(1.0)
                self.close_connection = True
                return
            run["events_count"] = len(self.run_store.get_events(run_id))
            self._send_json(200, run)
            return

        if route_path in {"/health", "/v1/health"}:
            self._send_json(
                200,
                {
                    "ok": True,
                    "gateway_model": self.router.gateway_model(),
                    "config_path": str(self.router.config_path),
                },
            )
            return

        if route_path in {"/-/status", "/v1/status"}:
            self.state.probe_if_due(force=False)
            self._send_json(200, self.state.status_payload())
            return

        if route_path in {"/-/discovery", "/v1/discovery"}:
            self.state.probe_if_due(force=True)
            status = self.state.status_payload()
            self._send_json(
                200,
                {
                    "ok": True,
                    "gateway_model": self.router.gateway_model(),
                    "lmstudio": status.get("lmstudio", []),
                },
            )
            return

        if route_path == "/v1/models":
            self.state.probe_if_due(force=False)
            status = self.state.status_payload()
            models: List[Dict[str, Any]] = [
                {
                    "id": self.router.gateway_model(),
                    "object": "model",
                    "created": int(time.time()),
                    "owned_by": "meta-cluster",
                }
            ]

            for node in status.get("lmstudio", []):
                if not isinstance(node, dict):
                    continue
                loaded = node.get("loaded_models", [])
                if not isinstance(loaded, list):
                    continue
                for model_id in loaded:
                    mid = str(model_id).strip()
                    if not mid:
                        continue
                    models.append(
                        {
                            "id": f"local/{mid}",
                            "object": "model",
                            "created": int(time.time()),
                            "owned_by": str(node.get("base_url", "lmstudio")),
                        }
                    )

            self._send_json(200, {"object": "list", "data": models})
            return

        self._send_json(404, {"error": {"message": "Not found", "type": "invalid_request_error"}})

    def _ordered_backends(self, route_name: str, provider_pin: Optional[str] = None) -> List[Backend]:
        backends = self.router.backends_for_route(route_name)
        if not backends:
            return []
        backends = [backend for backend in backends if not self.state.is_provider_circuit_open(backend.provider)]
        if provider_pin:
            pin = str(provider_pin).strip().lower()
            if pin.startswith("backend:"):
                backend_name = pin.split(":", 1)[1].strip()
                if not backend_name:
                    return []
                backends = [backend for backend in backends if backend.name == backend_name or backend.name.startswith(f"openai:{backend_name}:")]
            else:
                backends = [backend for backend in backends if backend.provider == pin]
            if not backends:
                return []
        self.state.probe_if_due(force=False)
        if not self.router.prefer_healthy():
            return backends
        healthy_backends: List[Backend] = []
        unhealthy_backends: List[Backend] = []
        for backend in backends:
            status = self.state.status_for_backend(backend.name)
            if status is None or status.get("healthy") is True:
                healthy_backends.append(backend)
            else:
                unhealthy_backends.append(backend)
        ordered = healthy_backends + unhealthy_backends
        cooled: List[Backend] = []
        active_rate_limited: List[Backend] = []
        for backend in ordered:
            until = self.state.provider_rate_limited_until(backend.provider)
            if until > time.time():
                active_rate_limited.append(backend)
            else:
                cooled.append(backend)
        return cooled + active_rate_limited

    def _validate_chat_request(self, body: Dict[str, Any]) -> Optional[str]:
        if body.get("stream") is True:
            return "Streaming is not supported yet. Set stream=false."
        messages = body.get("messages")
        if not isinstance(messages, list) or not messages:
            return "messages[] is required"
        return None

    def _provider_pin(self, body: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict):
            return None, None
        raw_pin = str(metadata.get("provider", "")).strip().lower()
        if not raw_pin:
            return None, None
        if raw_pin.startswith("backend:"):
            backend_name = raw_pin.split(":", 1)[1].strip()
            if not backend_name:
                return None, "metadata.provider backend pin must be backend:<name>"
            config = self.router.config_snapshot()
            backends = config.get("backends", {})
            if isinstance(backends, dict) and backend_name in backends:
                return f"backend:{backend_name}", None
            return None, f"Unknown backend pin: {backend_name}"
        if raw_pin in {"local", "openai", "gemini", "groq", "mistral", "cerebras", "openrouter", "pollinations"}:
            return raw_pin, None
        config = self.router.config_snapshot()
        backends = config.get("backends", {})
        if isinstance(backends, dict) and raw_pin in backends:
            return f"backend:{raw_pin}", None
        return None, "metadata.provider must be one of provider families or backend:<name>"

    def _verification_level(self, body: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        raw = str(metadata.get("verification_level", "")).strip().lower()
        if not raw:
            team_cfg = self.router.team_settings()
            raw = str(team_cfg.get("default_verification_level", "standard")).strip().lower() or "standard"
        if raw in {"light", "standard", "strict"}:
            return raw, None
        return "standard", "metadata.verification_level must be one of: light, standard, strict"

    def _cognitive_depth(self, body: Dict[str, Any]) -> Tuple[str, Optional[str]]:
        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        raw = str(metadata.get("cognitive_depth", "standard")).strip().lower() or "standard"
        if raw in {"low", "standard", "deep"}:
            return raw, None
        return "standard", "metadata.cognitive_depth must be one of: low, standard, deep"

    def _estimate_difficulty(self, body: Dict[str, Any], route_name: str) -> Dict[str, Any]:
        messages = body.get("messages", [])
        text = self.router._message_text(messages if isinstance(messages, list) else [])
        text_len = len(text)
        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        tools = body.get("tools", {})
        has_tools = isinstance(tools, dict) and any(k in tools for k in ["files", "web", "shell"])
        high_risk_keywords = [
            "verify",
            "proof",
            "strict",
            "production",
            "architecture",
            "root cause",
            "incident",
            "security",
            "legal",
            "risk",
            "benchmark",
            "optimize",
        ]
        hit_count = 0
        low_text = text.lower()
        for kw in high_risk_keywords:
            if kw in low_text:
                hit_count += 1
        kw_score = min(1.0, hit_count / 6.0)
        len_score = min(1.0, text_len / 1200.0)
        route_score = 0.15 if route_name in {"reasoning", "coding"} else 0.05
        depth_score = 0.2 if str(metadata.get("cognitive_depth", "")).strip().lower() == "deep" else 0.0
        tool_score = 0.15 if has_tools else 0.0
        explicit_strict = 0.2 if str(metadata.get("verification_level", "")).strip().lower() == "strict" else 0.0
        score = (0.30 * len_score) + (0.35 * kw_score) + route_score + depth_score + tool_score + explicit_strict
        score = max(0.0, min(1.0, score))
        return {
            "score": round(score, 4),
            "features": {
                "text_len": text_len,
                "len_score": round(len_score, 4),
                "keyword_hits": hit_count,
                "keyword_score": round(kw_score, 4),
                "route_name": route_name,
                "has_tools": has_tools,
                "cognitive_depth": str(metadata.get("cognitive_depth", "standard")).strip().lower() or "standard",
                "verification_level": str(metadata.get("verification_level", "")).strip().lower(),
            },
        }

    def _resolve_cognitive_policy(
        self,
        body: Dict[str, Any],
        route_name: str,
        explicit_team_endpoint: bool = False,
    ) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        body2 = dict(body)
        metadata2 = dict(metadata)
        body2["metadata"] = metadata2
        if not self.router.cognitive_routing_enabled():
            return body2, {"enabled": False}

        thresholds = self.router.cognitive_thresholds()
        profiles = self.router.cognitive_budget_profiles()
        manual_mode = str(metadata2.get("cognitive_mode", "")).strip().lower()
        allowed_modes = {"fast_path", "plan_solve", "debate_verify", "search_reflect"}
        if manual_mode not in allowed_modes:
            manual_mode = ""
        estimate = self._estimate_difficulty(body2, route_name)
        score = float(estimate.get("score", 0.0))
        if manual_mode:
            mode = manual_mode
            reason = "manual_override"
        elif score >= thresholds["search_reflect"]:
            mode = "search_reflect"
            reason = "difficulty_high_search_reflect"
        elif score >= thresholds["debate_verify"]:
            mode = "debate_verify"
            reason = "difficulty_high_debate_verify"
        elif score >= thresholds["plan_solve"]:
            mode = "plan_solve"
            reason = "difficulty_medium_plan_solve"
        else:
            mode = "fast_path"
            reason = "difficulty_low_fast_path"

        profile = dict(profiles.get(mode, profiles["plan_solve"]))
        team_mode_raw = str(metadata2.get("team_mode", "auto")).strip().lower()
        verification_raw = str(metadata2.get("verification_level", "")).strip().lower()
        explicit_team_mode = team_mode_raw in {"on", "off", "true", "false", "1", "0", "team", "solo"}
        explicit_verification = verification_raw in {"light", "standard", "strict"}
        explicit_max_tools = "max_tool_calls" in metadata2 or (
            isinstance(metadata2.get("tools_autonomy"), dict) and "max_tool_calls" in metadata2.get("tools_autonomy", {})
        )
        explicit_team_size = "team_size" in metadata2
        explicit_step_timeout = "team_step_timeout_seconds" in metadata2

        if not explicit_team_mode:
            metadata2["team_mode"] = "off" if mode == "fast_path" and not explicit_team_endpoint else "on"
        if not explicit_verification:
            metadata2["verification_level"] = str(profile.get("verification_level", "standard"))
        if not explicit_max_tools:
            metadata2["max_tool_calls"] = int(profile.get("max_tool_calls", 2))
        if not explicit_team_size:
            metadata2["team_size"] = int(profile.get("team_size", 3))
        if not explicit_step_timeout:
            metadata2["team_step_timeout_seconds"] = int(profile.get("step_timeout_seconds", self.router.team_step_timeout_seconds()))

        policy = {
            "enabled": True,
            "mode": mode,
            "reason": reason,
            "difficulty_score": score,
            "difficulty_features": estimate.get("features", {}),
            "budget_profile": {
                "team_size": int(metadata2.get("team_size", profile.get("team_size", 3))),
                "verification_level": str(metadata2.get("verification_level", profile.get("verification_level", "standard"))),
                "max_tool_calls": int(metadata2.get("max_tool_calls", profile.get("max_tool_calls", 2))),
                "team_step_timeout_seconds": int(metadata2.get("team_step_timeout_seconds", profile.get("step_timeout_seconds", self.router.team_step_timeout_seconds()))),
            },
            "applied_defaults": {
                "team_mode": not explicit_team_mode,
                "verification_level": not explicit_verification,
                "max_tool_calls": not explicit_max_tools,
                "team_size": not explicit_team_size,
                "team_step_timeout_seconds": not explicit_step_timeout,
            },
            "explicit_team_endpoint": explicit_team_endpoint,
        }
        return body2, policy

    def _extract_memory_updates(self, text: str) -> List[str]:
        section_items = self._extract_section_bullets(
            text,
            headings=["5) memory updates to store", "memory updates to store", "memory updates"],
            max_items=8,
        )
        if section_items:
            return section_items
        out: List[str] = []
        for line in text.splitlines():
            cleaned = line.strip()
            if not cleaned:
                continue
            if cleaned.endswith(":"):
                continue
            cleaned = re.sub(r"^\d+\.\s*", "", cleaned)
            cleaned = re.sub(r"^[-*]\s*", "", cleaned)
            low = cleaned.lower()
            if low.startswith(("direct answer", "next steps", "risks", "immediate validation", "memory updates", "open questions")):
                continue
            if len(cleaned) < 12:
                continue
            out.append(cleaned[:260])
            if len(out) >= 8:
                break
        return out

    def _extract_open_questions(self, text: str) -> List[str]:
        section_items = self._extract_section_bullets(
            text,
            headings=["6) open questions", "open questions"],
            max_items=5,
        )
        if section_items:
            return section_items
        out: List[str] = []
        for line in text.splitlines():
            cleaned = line.strip()
            if "?" not in cleaned:
                continue
            cleaned = re.sub(r"^\d+\.\s*", "", cleaned)
            cleaned = re.sub(r"^[-*]\s*", "", cleaned)
            if len(cleaned) < 8:
                continue
            out.append(cleaned[:220])
            if len(out) >= 5:
                break
        return out

    def _run_single_pass(
        self,
        body: Dict[str, Any],
        route_name: str,
        provider_pin: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
    ) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
        ordered = self._ordered_backends(route_name, provider_pin=provider_pin)
        if not ordered:
            pin_txt = f" with provider pin '{provider_pin}'" if provider_pin else ""
            return (
                500,
                {
                    "error": {
                        "message": f"No backends available for route '{route_name}'{pin_txt}",
                        "type": "server_error",
                    }
                },
                {},
            )

        failures: List[Dict[str, str]] = []
        timeout_s = max(5, int(timeout_seconds or self.router.request_timeout_seconds()))
        messages = body.get("messages", [])
        if not isinstance(messages, list):
            messages = []
        enable_tool_autonomy = self._tool_autonomy_enabled(body)
        for backend in ordered:
            start = time.time()
            try:
                text, latency_ms, tool_trace, tool_loop = self._call_backend_with_tool_autonomy(
                    backend=backend,
                    body=body,
                    messages=messages,
                    timeout_seconds=timeout_s,
                    enable_tool_autonomy=enable_tool_autonomy,
                )
                response = _build_openai_chat_response(
                    gateway_model=self.router.gateway_model(),
                    content=text,
                    upstream={},
                    backend_name=backend.name,
                )
                if tool_trace:
                    response["tool_trace"] = tool_trace
                response["tool_loop"] = tool_loop
                return 200, response, {
                    "x-router-route": route_name,
                    "x-router-backend": backend.name,
                    "x-router-provider": backend.provider,
                }
            except Exception as exc:
                latency_ms = int((time.time() - start) * 1000)
                err = str(exc)[:500]
                self.state.mark_runtime_result(backend=backend, ok=False, error=err, latency_ms=latency_ms)
                failures.append({"backend": backend.name, "error": err})
        return (
            502,
            {
                "error": {
                    "message": "All candidate backends failed",
                    "type": "upstream_error",
                    "route": route_name,
                    "details": failures,
                }
            },
            {},
        )

    def _call_backend_with_messages(
        self,
        backend: Backend,
        body: Dict[str, Any],
        messages: List[Dict[str, Any]],
        timeout_seconds: Optional[int] = None,
    ) -> Tuple[str, int, List[Dict[str, Any]], Dict[str, Any]]:
        return self._call_backend_with_tool_autonomy(
            backend=backend,
            body=body,
            messages=messages,
            timeout_seconds=max(5, int(timeout_seconds or self.router.team_step_timeout_seconds())),
            enable_tool_autonomy=self._tool_autonomy_enabled(body),
        )

    def _decide_team_mode(self, body: Dict[str, Any], explicit_team_endpoint: bool = False) -> bool:
        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        team_mode = str(metadata.get("team_mode", "auto")).strip().lower()
        verification_level = str(metadata.get("verification_level", "")).strip().lower()
        if team_mode in {"on", "true", "1", "team"}:
            return True
        if team_mode in {"off", "false", "0", "solo"}:
            return False
        if explicit_team_endpoint:
            return True
        if verification_level == "strict":
            return True

        team = self.router.team_settings()
        if not bool(team.get("enabled", True)):
            return False

        text = self.router._message_text(body.get("messages", []) if isinstance(body.get("messages"), list) else [])
        text_len = len(text)
        if text_len >= int(team.get("min_chars_for_team", 350)):
            return True

        keywords = team.get("team_keywords", [])
        if not isinstance(keywords, list):
            keywords = []
        for keyword in keywords:
            kw = str(keyword).strip().lower()
            if kw and kw in text:
                return True
        return False

    def _team_stage_system_prompt(self, stage: str, cognitive_mode: str, verification_level: str) -> str:
        base_prompts = {
            "planner": (
                "You are a planning specialist. Build a concise, testable plan with explicit checkpoints. "
                "Avoid long prose."
            ),
            "solver": (
                "You are the primary solver. Produce a direct, practical answer with clear assumptions "
                "and executable steps."
            ),
            "verifier": (
                "You are a strict verifier. Find correctness issues, unsupported claims, and missing tests. "
                "Prioritize factual and procedural accuracy."
            ),
            "strict_checker": (
                "You are a second-pass critical checker. Challenge weak assumptions and ensure the verification "
                "is rigorous before finalization."
            ),
            "synthesizer": (
                "You are the synthesis lead. Merge solver and verification outputs into one final answer, "
                "preserving concrete actions and caveats."
            ),
        }
        prompt = base_prompts.get(stage, "You are a helpful specialist.")
        if cognitive_mode == "plan_solve":
            prompt += " Favor speed and structure; keep outputs compact and decision-focused."
        elif cognitive_mode == "debate_verify":
            prompt += " Be adversarial on risk analysis and include uncertainty where evidence is thin."
        elif cognitive_mode == "search_reflect":
            prompt += " Prefer evidence-backed reasoning and explicitly call out what should be checked with tools."
        elif cognitive_mode == "fast_path":
            prompt += " Minimize token use and avoid unnecessary digressions."
        if verification_level == "strict" and stage in {"verifier", "strict_checker"}:
            prompt += " Treat unverified claims as failures and require corrective action."
        return prompt

    def _run_team_orchestration(
        self,
        body: Dict[str, Any],
        route_name: str,
        provider_pin: Optional[str] = None,
        verification_level: str = "standard",
        cognitive_policy: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
        def _fallback(reason: str) -> Tuple[int, Dict[str, Any], Dict[str, str]]:
            status, payload, headers = self._run_single_pass(body, route_name, provider_pin=provider_pin)
            next_headers = dict(headers)
            next_headers["x-router-team"] = "false"
            next_headers["x-router-team-fallback"] = reason
            if status == 200 and isinstance(payload, dict):
                payload["team"] = {
                    "used": False,
                    "route": route_name,
                    "provider_pin": provider_pin,
                    "verification_level": verification_level,
                    "fallback_reason": reason,
                    "cognitive": cognitive_policy or {},
                    "budget": {
                        "team_size": int(body.get("metadata", {}).get("team_size", 0))
                        if isinstance(body.get("metadata", {}), dict)
                        else 0,
                        "team_step_timeout_seconds": int(body.get("metadata", {}).get("team_step_timeout_seconds", self.router.team_step_timeout_seconds()))
                        if isinstance(body.get("metadata", {}), dict)
                        else self.router.team_step_timeout_seconds(),
                        "verification_level": verification_level,
                    },
                }
            return status, payload, next_headers

        ordered = self._ordered_backends(route_name, provider_pin=provider_pin)
        if not ordered:
            return _fallback("no_team_backends")

        metadata = body.get("metadata", {})
        if not isinstance(metadata, dict):
            metadata = {}
        team_cfg = self.router.team_settings()
        default_size = int(team_cfg.get("default_team_size", 3))
        if verification_level == "light":
            default_size = 2
        elif verification_level == "strict":
            default_size = max(default_size, 4)
        requested_size = int(metadata.get("team_size", default_size))
        team_step_timeout_seconds = max(5, int(metadata.get("team_step_timeout_seconds", self.router.team_step_timeout_seconds())))

        if verification_level == "light":
            team_size = max(2, min(4, requested_size))
        elif verification_level == "strict":
            team_size = max(4, min(5, requested_size))
        else:
            team_size = max(2, min(5, requested_size))

        unique_team: List[Backend] = []
        seen_names: set[str] = set()
        for backend in ordered:
            if backend.name in seen_names:
                continue
            seen_names.add(backend.name)
            unique_team.append(backend)
            if len(unique_team) >= team_size:
                break

        if len(unique_team) < 2:
            return _fallback("insufficient_team_members")

        query_text = self.router._message_text(body.get("messages", []))
        cog_mode = str((cognitive_policy or {}).get("mode", "")).strip().lower() or "plan_solve"
        planner = unique_team[0]
        solver = unique_team[1]
        verifier = unique_team[2] if len(unique_team) >= 3 else unique_team[0]
        strict_checker = unique_team[3] if len(unique_team) >= 4 else unique_team[-1]
        synthesizer = unique_team[0]
        stage_roles = self.router.team_stage_roles()
        planner = self._pick_stage_backend(unique_team, stage_roles.get("planner", "auto"), planner)
        solver = self._pick_stage_backend(unique_team, stage_roles.get("solver", "auto"), solver)
        verifier = self._pick_stage_backend(unique_team, stage_roles.get("verifier", "auto"), verifier)
        strict_checker = self._pick_stage_backend(unique_team, stage_roles.get("strict_checker", "auto"), strict_checker)
        synthesizer = self._pick_stage_backend(unique_team, stage_roles.get("synthesizer", "auto"), synthesizer)
        failures: List[Dict[str, str]] = []
        trace_steps: List[Dict[str, Any]] = []
        rate_limited_providers: set[str] = set()
        provider_usage: Dict[str, int] = {}
        provider_cooldown_until: Dict[str, float] = {}
        stage_rate_limit_cooldown_seconds = self.router.team_rate_limit_cooldown_seconds()
        prefer_provider_diversity = self.router.team_prefer_provider_diversity()
        planner_used = planner
        solver_used = solver
        verifier_used = verifier
        strict_used = strict_checker
        synth_used = synthesizer

        def _stage_candidates(primary: Backend) -> List[Backend]:
            seeded = [primary] + unique_team
            if not prefer_provider_diversity:
                out: List[Backend] = []
                seen: set[str] = set()
                for candidate in seeded:
                    if candidate.name in seen:
                        continue
                    seen.add(candidate.name)
                    out.append(candidate)
                return out
            now_ts = time.time()
            for candidate in seeded:
                provider = str(candidate.provider).strip().lower()
                if not provider:
                    continue
                global_until = self.state.provider_rate_limited_until(provider)
                if global_until > now_ts:
                    rate_limited_providers.add(provider)
                    current = float(provider_cooldown_until.get(provider, 0.0))
                    if global_until > current:
                        provider_cooldown_until[provider] = global_until
            return _rank_team_stage_candidates(
                ordered_candidates=seeded,
                primary_backend_name=primary.name,
                provider_usage=provider_usage,
                rate_limited_providers=rate_limited_providers,
                provider_cooldown_until=provider_cooldown_until,
                now_epoch=now_ts,
            )

        def _run_stage(
            stage: str,
            primary: Backend,
            stage_messages: List[Dict[str, Any]],
        ) -> Tuple[str, Backend]:
            last_error = ""
            for candidate in _stage_candidates(primary):
                try:
                    text, latency_ms, tools, tool_loop = self._call_backend_with_messages(
                        candidate, body, stage_messages, timeout_seconds=team_step_timeout_seconds
                    )
                    trace_steps.append(
                        {
                            "step": stage,
                            "backend": candidate.name,
                            "provider": candidate.provider,
                            "latency_ms": latency_ms,
                            "tool_calls": len(tools),
                            "tool_loop": tool_loop,
                        }
                    )
                    provider = str(candidate.provider).strip().lower()
                    if provider:
                        provider_usage[provider] = int(provider_usage.get(provider, 0)) + 1
                    return text, candidate
                except Exception as exc:
                    last_error = str(exc)[:500]
                    low = last_error.lower()
                    if "429" in low or "too many requests" in low:
                        provider = str(candidate.provider).strip().lower()
                        if provider:
                            rate_limited_providers.add(provider)
                            until = time.time() + stage_rate_limit_cooldown_seconds
                            current = float(provider_cooldown_until.get(provider, 0.0))
                            if until > current:
                                provider_cooldown_until[provider] = until
                            self.state.mark_provider_rate_limited(
                                provider=provider,
                                cooldown_seconds=stage_rate_limit_cooldown_seconds,
                                source="team_stage_429",
                            )
                    failures.append({"step": stage, "backend": candidate.name, "error": last_error})
            raise RuntimeError(last_error or f"{stage}_all_backends_failed")

        try:
            planner_messages = [
                {"role": "system", "content": self._team_stage_system_prompt("planner", cog_mode, verification_level)},
                {
                    "role": "user",
                    "content": (
                        "User request:\n"
                        f"{query_text}\n\n"
                        "Return:\n1) Plan (<=6 bullets)\n2) What to verify (<=4 bullets)"
                    ),
                },
            ]
            plan_text, planner_used = _run_stage("planner", planner, planner_messages)
        except Exception as exc:
            return _fallback("planner_failed")

        try:
            solver_messages = [
                {"role": "system", "content": self._team_stage_system_prompt("solver", cog_mode, verification_level)},
                {
                    "role": "user",
                    "content": (
                        "User request:\n"
                        f"{query_text}\n\n"
                        "Plan from planner:\n"
                        f"{plan_text}\n\n"
                        "Produce the best answer."
                    ),
                },
            ]
            draft_text, solver_used = _run_stage("solver", solver, solver_messages)
        except Exception as exc:
            return _fallback("solver_failed")

        verification_text = "Verdict: SKIPPED\nIssues found: none (light mode).\nCorrected answer: use solver draft."
        if verification_level != "light":
            try:
                verifier_messages = [
                    {"role": "system", "content": self._team_stage_system_prompt("verifier", cog_mode, verification_level)},
                    {
                        "role": "user",
                        "content": (
                            "User request:\n"
                            f"{query_text}\n\n"
                            "Draft answer:\n"
                            f"{draft_text}\n\n"
                            "Return:\n- Verdict: PASS or NEEDS_FIX\n- Issues found\n- Corrected answer"
                        ),
                    },
                ]
                verification_text, verifier_used = _run_stage("verifier", verifier, verifier_messages)
            except Exception as exc:
                if verification_level == "strict":
                    return _fallback("verifier_failed_strict")
                verification_text = "Verdict: UNKNOWN\nIssues found: verifier unavailable.\nCorrected answer: use solver draft."

        if verification_level == "strict":
            try:
                checker_messages = [
                    {"role": "system", "content": self._team_stage_system_prompt("strict_checker", cog_mode, verification_level)},
                    {
                        "role": "user",
                        "content": (
                            "User request:\n"
                            f"{query_text}\n\n"
                            "Solver draft:\n"
                            f"{draft_text}\n\n"
                            "Verifier output:\n"
                            f"{verification_text}\n\n"
                            "Return: PASS/NEEDS_FIX with final corrections."
                        ),
                    },
                ]
                strict_text, strict_used = _run_stage("strict_checker", strict_checker, checker_messages)
                verification_text = f"{verification_text}\n\nStrict check:\n{strict_text}"
            except Exception as exc:
                return _fallback("strict_checker_failed")

        try:
            final_messages = [
                {
                    "role": "system",
                    "content": self._team_stage_system_prompt("synthesizer", cog_mode, verification_level),
                },
                {
                    "role": "user",
                    "content": (
                        "User request:\n"
                        f"{query_text}\n\n"
                        "Solver draft:\n"
                        f"{draft_text}\n\n"
                        "Verification notes:\n"
                        f"{verification_text}\n\n"
                        "Return the final answer and include a brief verification summary at the end."
                    ),
                },
            ]
            final_text, synth_used = _run_stage("synthesizer", synthesizer, final_messages)
        except Exception as exc:
            synth_used = solver_used
            final_text = (
                f"{draft_text}\n\nVerification summary:\n"
                f"{verification_text[:1200]}"
            )

        response = _build_openai_chat_response(
            gateway_model=self.router.gateway_model(),
            content=final_text,
            upstream={},
            backend_name=synth_used.name,
        )
        response["team"] = {
            "used": True,
            "route": route_name,
            "provider_pin": provider_pin,
            "verification_level": verification_level,
            "stage_roles": stage_roles,
            "stage_assignments": {
                "planner": planner_used.name,
                "solver": solver_used.name,
                "verifier": verifier_used.name,
                "strict_checker": strict_used.name,
                "synthesizer": synth_used.name,
            },
            "members": [member.name for member in unique_team],
            "steps": trace_steps,
            "failures": failures,
            "cognitive": cognitive_policy or {},
            "budget": {
                "team_size": team_size,
                "team_step_timeout_seconds": team_step_timeout_seconds,
                "verification_level": verification_level,
            },
            "rate_limited_providers": sorted(rate_limited_providers),
            "provider_concentration": _provider_concentration_metrics(trace_steps),
            "rate_limit_cooldowns": {
                provider: max(0, int(until - time.time()))
                for provider, until in sorted(provider_cooldown_until.items())
                if until > time.time()
            },
        }
        return 200, response, {
            "x-router-route": route_name,
            "x-router-backend": synth_used.name,
            "x-router-provider": synth_used.provider,
            "x-router-team": "true",
        }

    def do_POST(self) -> None:  # noqa: N802
        route_path = urlparse(self.path).path
        if route_path != "/":
            route_path = route_path.rstrip("/")
        self._route_path = route_path
        self.event_logger.log(
            "http_request",
            {
                "request_id": self._ensure_request_id(),
                "method": "POST",
                "path": route_path,
            },
        )
        allowed, denial = self._check_admin_access(route_path)
        if not allowed:
            code, payload, headers = denial if denial is not None else (403, {"error": {"message": "Forbidden"}}, {})
            self._send_json(code, payload, extra_headers=headers)
            return

        if route_path in {
            "/-/admin/api/provider/upsert",
            "/-/admin/api/provider/delete",
            "/-/admin/api/provider/set-enabled",
            "/-/admin/api/provider/test",
            "/-/admin/api/team/stage-roles",
            "/-/admin/api/cognitive-routing",
            "/-/admin/api/local-model/upsert",
            "/-/admin/api/local-model/delete",
            "/-/admin/api/local-model/set-enabled",
            "/-/admin/api/chat",
            "/-/admin/api/reload",
        }:
            body: Dict[str, Any] = {}
            if route_path != "/-/admin/api/reload":
                try:
                    body = self._read_json()
                except Exception as exc:
                    self._send_json(400, {"error": {"message": f"Invalid JSON: {exc}", "type": "invalid_request_error"}})
                    return
            if route_path == "/-/admin/api/provider/upsert":
                code, payload = self._admin_upsert_provider(body)
                self._send_json(code, payload)
                return
            if route_path == "/-/admin/api/provider/delete":
                code, payload = self._admin_remove_provider(body)
                self._send_json(code, payload)
                return
            if route_path == "/-/admin/api/provider/set-enabled":
                code, payload = self._admin_set_provider_enabled(body)
                self._send_json(code, payload)
                return
            if route_path == "/-/admin/api/provider/test":
                code, payload = self._admin_test_provider(body)
                self._send_json(code, payload)
                return
            if route_path == "/-/admin/api/team/stage-roles":
                code, payload = self._admin_update_team_stage_roles(body)
                self._send_json(code, payload)
                return
            if route_path == "/-/admin/api/cognitive-routing":
                code, payload = self._admin_update_cognitive_routing(body)
                self._send_json(code, payload)
                return
            if route_path == "/-/admin/api/local-model/upsert":
                code, payload = self._admin_upsert_local_model(body)
                self._send_json(code, payload)
                return
            if route_path == "/-/admin/api/local-model/delete":
                code, payload = self._admin_delete_local_model(body)
                self._send_json(code, payload)
                return
            if route_path == "/-/admin/api/local-model/set-enabled":
                code, payload = self._admin_set_local_model_enabled(body)
                self._send_json(code, payload)
                return
            if route_path == "/-/admin/api/chat":
                code, payload = self._admin_chat_preview(body)
                self._send_json(code, payload)
                return
            if route_path == "/-/admin/api/reload":
                try:
                    self.router.reload()
                    self.state.reset()
                    self.state.probe_if_due(force=True)
                except Exception as exc:
                    self._send_json(500, {"error": {"message": f"Reload failed: {exc}", "type": "server_error"}})
                    return
                self._send_json(200, self._admin_bootstrap_payload())
                return

        is_work_route, run_id, subroute = self._work_route(route_path)
        if is_work_route:
            if run_id and subroute == "cancel":
                run = self.run_store.get_run(run_id)
                if not isinstance(run, dict):
                    self._send_json(404, {"error": {"message": f"Run not found: {run_id}", "type": "invalid_request_error"}})
                    return
                status = str(run.get("status", "")).strip().lower()
                if status in {"done", "failed", "canceled"}:
                    self._send_json(409, {"error": {"message": f"Run already finalized ({status})", "type": "invalid_request_error"}})
                    return
                self.run_store.request_cancel(run_id)
                self.run_store.update_run(run_id, {"status": "canceling", "updated_at": int(time.time())})
                self.run_store.append_event(run_id, "cancel_requested", {"reason": "user_requested"})
                self._send_json(200, {"ok": True, "id": run_id, "status": "canceling"})
                return
            if run_id and subroute == "approve":
                run = self.run_store.get_run(run_id)
                if not isinstance(run, dict):
                    self._send_json(404, {"error": {"message": f"Run not found: {run_id}", "type": "invalid_request_error"}})
                    return
                request_api_key = self._extract_request_api_key()
                auth_ok, auth_err = self._is_privileged_action_allowed("approve", request_api_key)
                if not auth_ok:
                    code = 503 if auth_err == "privileged_auth_misconfigured" else 403
                    self._send_json(code, {"error": {"message": f"Access denied for approve ({auth_err})", "type": "invalid_request_error"}})
                    return
                try:
                    body = self._read_json()
                except Exception as exc:
                    self._send_json(400, {"error": {"message": f"Invalid JSON: {exc}", "type": "invalid_request_error"}})
                    return
                pending = run.get("pending_approval", {})
                if not isinstance(pending, dict):
                    self._send_json(409, {"error": {"message": "No pending approval plan for this run", "type": "invalid_request_error"}})
                    return
                expires_at = int(pending.get("expires_at", 0))
                if expires_at > 0 and int(time.time()) > expires_at:
                    self.run_store.update_run(run_id, {"pending_approval": None, "updated_at": int(time.time())})
                    self.run_store.append_event(run_id, "approval_expired", {"expired_at": expires_at})
                    self._send_json(410, {"error": {"message": "Approval token expired", "type": "invalid_request_error"}})
                    return
                token = str(body.get("token", "")).strip()
                if not token:
                    self._send_json(400, {"error": {"message": "Missing approval token", "type": "invalid_request_error"}})
                    return
                claims, verify_err = self._verify_approval_token(token, run_id)
                if verify_err:
                    code = 410 if verify_err == "token_expired" else 403
                    self._send_json(code, {"error": {"message": "Invalid approval token", "type": "invalid_request_error"}})
                    return
                token_id = str((claims or {}).get("jti", "")).strip()
                claim_status, claimed = self.run_store.claim_pending_approval(run_id, token_id=token_id, now_ts=int(time.time()))
                if claim_status == "expired":
                    self.run_store.append_event(run_id, "approval_expired", {"expired_at": expires_at})
                    self._send_json(410, {"error": {"message": "Approval token expired", "type": "invalid_request_error"}})
                    return
                if claim_status == "used":
                    self._send_json(409, {"error": {"message": "Approval token already used", "type": "invalid_request_error"}})
                    return
                if claim_status in {"mismatch", "no_pending", "run_missing"}:
                    self._send_json(403, {"error": {"message": "Invalid approval token", "type": "invalid_request_error"}})
                    return
                organize_plan = (claimed or {}).get("organize_plan", {})
                if not isinstance(organize_plan, dict):
                    self._send_json(409, {"error": {"message": "Stored organize plan is invalid", "type": "invalid_request_error"}})
                    return
                exec_result = self._execute_organize_plan(organize_plan, apply_changes=True)
                applied = {
                    "ok": True,
                    "id": run_id,
                    "status": "approved_and_applied",
                    "changed_files": exec_result.get("changed_files", []),
                    "operations": exec_result.get("operations", []),
                }
                self.run_store.update_run(run_id, {"approval_applied": applied, "updated_at": int(time.time())})
                self.run_store.append_event(run_id, "approval_applied", {"changed_files": len(applied["changed_files"])})
                self._send_json(200, applied)
                return

            if route_path != "/v1/work":
                self._send_json(404, {"error": {"message": "Not found", "type": "invalid_request_error"}})
                return

            try:
                body = self._read_json()
            except Exception as exc:
                self._send_json(400, {"error": {"message": f"Invalid JSON: {exc}", "type": "invalid_request_error"}})
                return
            request_api_key = self._extract_request_api_key()
            body["_request_api_key"] = request_api_key

            new_run_id = f"run_{uuid.uuid4().hex[:12]}"
            mode = str(body.get("mode", "research")).strip().lower() or "research"
            metadata = body.get("metadata", {})
            if not isinstance(metadata, dict):
                metadata = {}
            async_mode = bool(metadata.get("async", False))
            run = {
                "id": new_run_id,
                "object": "work.run",
                "status": "queued" if async_mode else "running",
                "mode": mode,
                "project_id": str(body.get("project_id", "default-project")).strip() or "default-project",
                "objective": str(body.get("objective", "")).strip(),
                "created_at": int(time.time()),
                "updated_at": int(time.time()),
                "async": async_mode,
            }
            self.run_store.create_run(run)
            self.run_store.append_event(new_run_id, "started", {"mode": mode})
            if async_mode:
                self.work_executor.submit(self._execute_and_finalize_run, new_run_id, body)
                self.run_store.append_event(
                    new_run_id,
                    "queued",
                    {"pending_jobs": self.work_executor.pending(), "max_concurrency": self.work_executor.max_workers},
                )
                run_now = self.run_store.get_run(new_run_id) or run
                self._send_json(202, run_now)
                return

            self._execute_and_finalize_run(new_run_id, body)
            final_run = self.run_store.get_run(new_run_id) or {"id": new_run_id, "status": "failed"}
            final_status = str(final_run.get("status", "failed")).strip().lower()
            if final_status == "done":
                code = 201
            elif final_status == "canceled":
                code = 409
            else:
                raw_error_status = int(final_run.get("error_status", 502))
                code = raw_error_status if 400 <= raw_error_status <= 599 else 502
            self._send_json(code, final_run)
            return

        if route_path == "/-/reload":
            try:
                self.router.reload()
                self.state.reset()
                self.state.probe_if_due(force=True)
            except Exception as exc:
                self._send_json(500, {"error": {"message": f"Reload failed: {exc}", "type": "server_error"}})
                return
            self._send_json(200, {"ok": True, "gateway_model": self.router.gateway_model()})
            return

        if route_path not in {"/v1/chat/completions", "/v1/team/chat/completions"}:
            self._send_json(404, {"error": {"message": "Not found", "type": "invalid_request_error"}})
            return

        try:
            body = self._read_json()
        except Exception as exc:
            self._send_json(400, {"error": {"message": f"Invalid JSON: {exc}", "type": "invalid_request_error"}})
            return

        validation_error = self._validate_chat_request(body)
        if validation_error:
            self._send_json(400, {"error": {"message": validation_error, "type": "invalid_request_error"}})
            return

        route_name = self.router.pick_route_name(body)
        explicit_team_endpoint = route_path == "/v1/team/chat/completions"
        body, cognitive_policy = self._resolve_cognitive_policy(
            body,
            route_name=route_name,
            explicit_team_endpoint=explicit_team_endpoint,
        )

        provider_pin, provider_error = self._provider_pin(body)
        if provider_error:
            self._send_json(400, {"error": {"message": provider_error, "type": "invalid_request_error"}})
            return
        verification_level, verification_error = self._verification_level(body)
        if verification_error:
            self._send_json(400, {"error": {"message": verification_error, "type": "invalid_request_error"}})
            return

        use_team = self._decide_team_mode(body, explicit_team_endpoint=explicit_team_endpoint)

        if use_team:
            status, payload, headers = self._run_team_orchestration(
                body,
                route_name,
                provider_pin=provider_pin,
                verification_level=verification_level,
                cognitive_policy=cognitive_policy,
            )
            self._send_json(status, payload, extra_headers=headers)
            return

        status, payload, headers = self._run_single_pass(body, route_name, provider_pin=provider_pin)
        headers = dict(headers)
        headers["x-router-team"] = "false"
        if status == 200 and isinstance(payload, dict):
            payload["team"] = {
                "used": False,
                "route": route_name,
                "provider_pin": provider_pin,
                "verification_level": verification_level,
                "cognitive": cognitive_policy,
            }
        self._send_json(status, payload, extra_headers=headers)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OpenAI-compatible model cluster gateway")
    parser.add_argument(
        "--config",
        default=str(project_root() / "config" / "gateway_config.json"),
        help="Gateway JSON config path",
    )
    parser.add_argument("--host", default="0.0.0.0", help="Bind host")
    parser.add_argument("--port", type=int, default=4010, help="Bind port")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = _abs_path(args.config)
    router = GatewayRouter(config_path)
    event_logger = GatewayEventLogger(
        path=router.logging_path(),
        enabled=router.logging_enabled(),
        max_payload_chars=router.logging_max_payload_chars(),
        redact_secrets=router.logging_redact_secrets(),
    )
    state = ClusterState(router, event_logger=event_logger)
    run_store = RunStore(project_root() / "artifacts" / "runs", event_logger=event_logger)
    work_executor = WorkExecutor(router.work_max_concurrency())
    memory_store = ProjectMemoryStore(project_root() / "artifacts" / "memory" / "project_memory.json")

    OpenAICompatibleHandler.router = router
    OpenAICompatibleHandler.state = state
    OpenAICompatibleHandler.run_store = run_store
    OpenAICompatibleHandler.work_executor = work_executor
    OpenAICompatibleHandler.memory_store = memory_store
    OpenAICompatibleHandler.event_logger = event_logger

    server = ThreadingHTTPServer((args.host, args.port), OpenAICompatibleHandler)
    print(
        json.dumps(
            {
                "status": "starting",
                "host": args.host,
                "port": args.port,
                "gateway_model": router.gateway_model(),
                "config": str(config_path),
                "event_log": str(router.logging_path()),
                "event_log_enabled": router.logging_enabled(),
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
