#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List

import requests

from model_cluster_gateway import (
    Backend,
    ClusterState,
    GatewayRouter,
    OpenAICompatibleHandler,
    _provider_concentration_metrics,
    _rank_team_stage_candidates,
)


def _assert(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)


def test_kimi_tool_tag_parsing() -> None:
    payload = (
        "I'll search first.<|tool_calls_section_begin|><|tool_call_begin|>functions.web_search:0"
        "<|tool_call_argument_begin|>{\"queries\":[\"gateway team routing\"]}"
        "<|tool_call_end|><|tool_calls_section_end|>"
    )
    out = OpenAICompatibleHandler._extract_tool_request(object(), payload)
    _assert(isinstance(out, dict), "tool request not parsed")
    _assert(out.get("tool") == "web_search", "tool name parse mismatch")
    _assert(out.get("queries") == ["gateway team routing"], "tool args parse mismatch")


def test_rank_candidates_prefers_diversity_and_avoids_cooldown() -> None:
    backends = [
        Backend(name="b1", kind="openai", model="m", base_url="u", provider="gemini"),
        Backend(name="b2", kind="openai", model="m", base_url="u", provider="groq"),
        Backend(name="b3", kind="openai", model="m", base_url="u", provider="gemini"),
    ]
    now = time.time()
    ranked = _rank_team_stage_candidates(
        ordered_candidates=backends,
        primary_backend_name="b1",
        provider_usage={"gemini": 2, "groq": 0},
        rate_limited_providers={"gemini"},
        provider_cooldown_until={"gemini": now + 20},
        now_epoch=now,
    )
    _assert(len(ranked) == 3, "ranked candidate length mismatch")
    _assert(ranked[0].provider == "groq", "cooldown provider should be deferred")


def test_provider_concentration_metrics() -> None:
    metrics = _provider_concentration_metrics(
        [
            {"provider": "groq"},
            {"provider": "groq"},
            {"provider": "mistral"},
            {"provider": "groq"},
        ]
    )
    _assert(metrics.get("unique_providers") == 2, "unique provider count mismatch")
    _assert(metrics.get("stage_count") == 4, "stage count mismatch")
    _assert(abs(float(metrics.get("max_share", 0.0)) - 0.75) < 0.001, "max share mismatch")


def test_cluster_state_rate_limit_tracking() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="gateway-hardening-"))
    cfg = tmpdir / "gateway_config.json"
    cfg.write_text(
        json.dumps(
            {
                "gateway_model": "meta-cluster-1",
                "routing": {"routes": []},
                "backends": {},
                "team_orchestration": {"rate_limit_cooldown_seconds": 7},
            }
        ),
        encoding="utf-8",
    )
    router = GatewayRouter(cfg)
    state = ClusterState(router)
    state.mark_provider_rate_limited("gemini", cooldown_seconds=7, source="test")
    until = state.provider_rate_limited_until("gemini")
    _assert(until > time.time(), "provider cooldown not marked")


def run_live_partner_check(base_url: str, admin_user: str, admin_pass: str, trials: int) -> Dict[str, Any]:
    prompt = (
        "A team has 3 providers with intermittent 429s. Give 3 concrete routing hardening steps "
        "with one metric each."
    )
    latencies: List[float] = []
    team_runs = 0
    concentration_samples: List[float] = []

    for _ in range(trials):
        started = time.time()
        resp = requests.post(
            base_url.rstrip("/") + "/v1/team/chat/completions",
            timeout=120,
            json={
                "model": "meta-cluster-1",
                "messages": [{"role": "user", "content": prompt}],
                "metadata": {
                    "team_mode": "on",
                    "router_hint": "reasoning",
                    "verification_level": "standard",
                },
            },
        )
        elapsed = time.time() - started
        latencies.append(elapsed)
        body = resp.json() if resp.text.strip() else {}
        _assert(resp.status_code == 200, f"live team run failed: {resp.status_code}")
        team = body.get("team", {}) if isinstance(body, dict) else {}
        if bool(team.get("used")):
            team_runs += 1
        concentration = team.get("provider_concentration", {}) if isinstance(team, dict) else {}
        if isinstance(concentration, dict):
            try:
                concentration_samples.append(float(concentration.get("max_share", 0.0)))
            except Exception:
                pass

    partner = requests.post(
        base_url.rstrip("/") + "/v1/work",
        timeout=180,
        json={
            "mode": "research",
            "project_id": "model-cluster-gateway",
            "objective": "Review current routing hardening and propose 3 next validation checks.",
            "metadata": {
                "provider": "backend:doubleword",
                "team_mode": "on",
                "verification_level": "standard",
            },
        },
    )
    partner_body = partner.json() if partner.text.strip() else {}
    _assert(partner.status_code in {200, 201}, f"work partner run failed: {partner.status_code}")

    lat_sorted = sorted(latencies)
    avg = sum(latencies) / len(latencies) if latencies else 0.0
    p50 = lat_sorted[len(lat_sorted) // 2] if lat_sorted else 0.0
    p95 = lat_sorted[min(len(lat_sorted) - 1, int(len(lat_sorted) * 0.95))] if lat_sorted else 0.0
    return {
        "trials": trials,
        "team_runs": team_runs,
        "latency_seconds": {
            "avg": round(avg, 4),
            "p50": round(p50, 4),
            "p95": round(p95, 4),
            "min": round(min(latencies), 4) if latencies else 0.0,
            "max": round(max(latencies), 4) if latencies else 0.0,
        },
        "provider_concentration_max_share_avg": round(sum(concentration_samples) / len(concentration_samples), 4)
        if concentration_samples
        else 0.0,
        "work_partner_status": partner_body.get("status"),
        "work_partner_run_id": partner_body.get("id"),
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Hardening regression checks for model-cluster-gateway")
    p.add_argument("--base-url", default="http://10.0.0.15:4010", help="Gateway base URL")
    p.add_argument("--admin-user", default="admin", help="Admin username for /-/admin/api/chat")
    p.add_argument("--admin-pass", default="adminpass", help="Admin password for /-/admin/api/chat")
    p.add_argument("--live", action="store_true", help="Run live partner and benchmark checks against gateway")
    p.add_argument("--trials", type=int, default=3, help="Live team-mode trial count")
    p.add_argument("--out", default="artifacts/dogfood/hardening_regression_latest.json", help="Output report path")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    checks = [
        ("kimi_tool_tag_parsing", test_kimi_tool_tag_parsing),
        ("candidate_ranking_diversity", test_rank_candidates_prefers_diversity_and_avoids_cooldown),
        ("provider_concentration_metrics", test_provider_concentration_metrics),
        ("cluster_rate_limit_tracking", test_cluster_state_rate_limit_tracking),
    ]
    report: Dict[str, Any] = {
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "ok": True,
        "checks": [],
        "live": None,
    }

    for name, fn in checks:
        try:
            fn()
            report["checks"].append({"name": name, "ok": True})
        except Exception as exc:
            report["ok"] = False
            report["checks"].append({"name": name, "ok": False, "error": str(exc)[:400]})

    if args.live:
        try:
            report["live"] = run_live_partner_check(args.base_url, args.admin_user, args.admin_pass, max(1, args.trials))
        except Exception as exc:
            report["ok"] = False
            report["live"] = {"ok": False, "error": str(exc)[:400]}

    report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    out_path = Path(args.out)
    if not out_path.is_absolute():
        out_path = Path(__file__).resolve().parents[1] / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"ok": report["ok"], "checks": report["checks"], "live": report["live"]}, ensure_ascii=False))
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
