#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def _looks_like_action_plan(text: str) -> bool:
    if not text.strip():
        return False
    if re.search(r"\bnext step", text, flags=re.IGNORECASE):
        return True
    if re.search(r"^\s*(?:\d+\.|-)\s+\S+", text, flags=re.MULTILINE):
        return True
    return False


def _normalize_line(text: str) -> str:
    return re.sub(r"\s+", " ", text.strip()).lower()


def _is_noisy_memory_item(text: str) -> bool:
    t = _normalize_line(text)
    if len(t) < 16:
        return True
    weak_patterns = [
        "n/a",
        "none",
        "unknown",
        "tbd",
        "follow up",
        "need more context",
        "more details needed",
        "pending",
    ]
    return any(p in t for p in weak_patterns)


def _extraction_metrics(next_actions: List[str], memory_updates: List[str], open_questions: List[str]) -> Dict[str, Any]:
    noisy = sum(1 for item in memory_updates if _is_noisy_memory_item(str(item)))
    memory_noise_ratio = (noisy / len(memory_updates)) if memory_updates else 1.0
    coverage = 0.0
    if next_actions:
        coverage += 0.45
    if memory_updates:
        coverage += 0.45
    if open_questions:
        coverage += 0.10
    score = max(0.0, coverage * (1.0 - memory_noise_ratio))
    return {
        "next_actions_count": len(next_actions),
        "memory_updates_count": len(memory_updates),
        "open_questions_count": len(open_questions),
        "memory_update_noise_ratio": round(memory_noise_ratio, 4),
        "extraction_quality_score": round(score, 4),
    }


def _chat(
    base_url: str,
    endpoint: str,
    body: Dict[str, Any],
    timeout_seconds: int,
) -> Tuple[int, Dict[str, Any], Dict[str, str], str]:
    url = base_url.rstrip("/") + endpoint
    started = time.time()
    resp = requests.post(url, json=body, timeout=timeout_seconds)
    elapsed_ms = int((time.time() - started) * 1000)
    headers = {k.lower(): v for k, v in resp.headers.items()}
    payload: Dict[str, Any]
    try:
        raw = resp.json()
        payload = raw if isinstance(raw, dict) else {"raw": raw}
    except Exception:
        payload = {"raw_text": resp.text[:4000]}
    headers["x-elapsed-ms"] = str(elapsed_ms)
    return resp.status_code, payload, headers, url


def _get_json(base_url: str, endpoint: str, timeout_seconds: int) -> Tuple[int, Dict[str, Any], Dict[str, str], str]:
    url = base_url.rstrip("/") + endpoint
    started = time.time()
    resp = requests.get(url, timeout=timeout_seconds)
    elapsed_ms = int((time.time() - started) * 1000)
    headers = {k.lower(): v for k, v in resp.headers.items()}
    try:
        raw = resp.json()
        payload = raw if isinstance(raw, dict) else {"raw": raw}
    except Exception:
        payload = {"raw_text": resp.text[:4000]}
    headers["x-elapsed-ms"] = str(elapsed_ms)
    return resp.status_code, payload, headers, url


def _extract_assistant_text(payload: Dict[str, Any]) -> str:
    try:
        content = payload["choices"][0]["message"]["content"]
    except Exception:
        return ""
    if isinstance(content, str):
        return content.strip()
    return str(content).strip()


def run(args: argparse.Namespace) -> int:
    project = args.project.strip() or "model-cluster-gateway"
    providers = [p.strip().lower() for p in args.providers.split(",") if p.strip()]
    report: Dict[str, Any] = {
        "started_at": _utc_now(),
        "base_url": args.base_url,
        "project": project,
        "providers": providers,
        "scenarios": [],
        "summary": {},
    }

    failures: List[str] = []
    provider_passes = 0

    work_body = {
        "mode": "research",
        "project_id": project,
        "objective": "Assess immediate priorities for this project and return concrete next actions.",
        "metadata": {
            "team_mode": "on",
            "verification_level": "standard",
            "provider": args.work_provider,
            "team_size": 4,
        },
    }
    code, payload, headers, url = _chat(
        base_url=args.base_url,
        endpoint="/v1/work",
        body=work_body,
        timeout_seconds=args.timeout_seconds,
    )
    result = payload.get("result", {}) if isinstance(payload.get("result"), dict) else {}
    text = str(result.get("final_answer", "")).strip()
    next_actions = result.get("next_actions", []) if isinstance(result.get("next_actions"), list) else []
    memory_updates = result.get("memory_updates", []) if isinstance(result.get("memory_updates"), list) else []
    open_questions = result.get("open_questions", []) if isinstance(result.get("open_questions"), list) else []
    extraction = _extraction_metrics(next_actions, memory_updates, open_questions)
    run_id = str(payload.get("id", "")).strip()
    events_count = 0
    if run_id:
        e_code, e_payload, _, e_url = _get_json(
            base_url=args.base_url,
            endpoint=f"/v1/work/{run_id}/events",
            timeout_seconds=args.timeout_seconds,
        )
        if e_code == 404:
            events_count = 0
        elif isinstance(e_payload.get("events"), list):
            events_count = len(e_payload["events"])
        else:
            events_count = 0
    else:
        e_url = ""
    ok = code in {200, 201} and str(payload.get("status", "")) == "done" and len(text) >= 40 and len(next_actions) >= 1
    extraction_ok = (
        extraction["memory_update_noise_ratio"] <= args.max_memory_noise_ratio
        and extraction["extraction_quality_score"] >= args.min_extraction_quality_score
    )
    report["scenarios"].append(
        {
            "name": "work_project_cycle",
            "url": url,
            "status_code": code,
            "ok": ok,
            "elapsed_ms": int(headers.get("x-elapsed-ms", "0")),
            "run_id": run_id,
            "events_url": e_url,
            "events_count": events_count,
            "mode": payload.get("mode"),
            "run_status": payload.get("status"),
            "preview": text[:400],
            "extraction_metrics": extraction,
            "extraction_ok": extraction_ok,
            "error": payload.get("error"),
        }
    )
    if not ok:
        failures.append("work_project_cycle")

    for provider in providers:
        body = {
            "model": "meta-cluster-1",
            "messages": [{"role": "user", "content": f"Project '{project}': propose one next action in one sentence."}],
            "metadata": {
                "team_mode": "off",
                "provider": provider,
                "router_hint": "reasoning",
            },
        }
        code, payload, headers, url = _chat(
            base_url=args.base_url,
            endpoint="/v1/chat/completions",
            body=body,
            timeout_seconds=args.timeout_seconds,
        )
        text = _extract_assistant_text(payload)
        ok = code == 200 and len(text) >= 8
        if ok:
            provider_passes += 1
        report["scenarios"].append(
            {
                "name": f"provider_pin_{provider}",
                "url": url,
                "status_code": code,
                "ok": ok,
                "elapsed_ms": int(headers.get("x-elapsed-ms", "0")),
                "router_provider": headers.get("x-router-provider", ""),
                "router_backend": headers.get("x-router-backend", ""),
                "preview": text[:220],
                "error": payload.get("error"),
            }
        )

    if provider_passes < args.min_provider_passes:
        failures.append(
            f"provider_threshold<{args.min_provider_passes} (got {provider_passes})"
        )

    report["finished_at"] = _utc_now()
    report["summary"] = {
        "ok": len(failures) == 0,
        "failures": failures,
        "provider_passes": provider_passes,
        "provider_total": len(providers),
        "min_provider_passes": args.min_provider_passes,
        "work_extraction_quality_score": extraction["extraction_quality_score"],
        "work_memory_update_noise_ratio": extraction["memory_update_noise_ratio"],
        "work_extraction_ok": extraction_ok,
    }

    out_path = Path(args.out).expanduser().resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(report["summary"], ensure_ascii=False))
    return 0 if len(failures) == 0 else 1


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Periodic dogfood workflow check for model-cluster-gateway")
    p.add_argument("--base-url", default="http://127.0.0.1:4010", help="Gateway base URL")
    p.add_argument("--project", default="model-cluster-gateway", help="Project identifier used in prompts")
    p.add_argument(
        "--providers",
        default="groq,mistral,cerebras,openrouter",
        help="Comma-separated provider pins to verify",
    )
    p.add_argument("--min-provider-passes", type=int, default=2, help="Minimum provider pin checks that must pass")
    p.add_argument("--timeout-seconds", type=int, default=90, help="Request timeout for each scenario")
    p.add_argument("--work-provider", default="mistral", help="Provider pin for /v1/work scenario")
    p.add_argument(
        "--max-memory-noise-ratio",
        type=float,
        default=1.0,
        help="Threshold for extraction memory-update noise ratio (used for extraction_ok reporting)",
    )
    p.add_argument(
        "--min-extraction-quality-score",
        type=float,
        default=0.0,
        help="Threshold for extraction quality score (used for extraction_ok reporting)",
    )
    p.add_argument(
        "--out",
        default="artifacts/dogfood/latest_report.json",
        help="Output JSON report path",
    )
    return p.parse_args()


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
