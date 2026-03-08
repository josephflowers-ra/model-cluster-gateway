#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_key(path: str, env_name: str) -> str:
    env_val = os.getenv(env_name, "").strip()
    if env_val:
        return env_val
    p = Path(path).expanduser()
    if p.exists():
        return p.read_text(encoding="utf-8").strip()
    return ""


def default_prompts() -> List[Tuple[str, str]]:
    return [
        (
            "coding_patch",
            "You are reviewing a Python API. Give exactly 3 concrete next coding steps to reduce 502 errors.",
        ),
        (
            "research_plan",
            "Give a concise research plan (4 bullets) for benchmarking model routing reliability in a multi-provider gateway.",
        ),
        (
            "work_memory_extract",
            (
                "Return exactly these sections with concise bullets:\n"
                "Next steps:\n"
                "- ...\n"
                "Memory updates:\n"
                "- ...\n"
                "Open questions:\n"
                "- ...\n"
                "Context: model-cluster-gateway reliability and workflow hardening."
            ),
        ),
    ]


def build_cases(args: argparse.Namespace) -> List[Dict[str, Any]]:
    cases: List[Dict[str, Any]] = [
        {
            "provider": "openai",
            "base_url": "https://api.openai.com/v1",
            "model": args.openai_model,
            "api_key": read_key(args.openai_key_file, "OPENAI_API_KEY"),
        },
        {
            "provider": "openrouter",
            "base_url": "https://openrouter.ai/api/v1",
            "model": args.openrouter_model,
            "api_key": read_key(args.openrouter_key_file, "OPENROUTER_API_KEY"),
            "extra_headers": {
                "HTTP-Referer": "https://local.codex.gateway",
                "X-Title": "model-cluster-gateway-benchmark",
            },
        },
    ]
    if args.include_local:
        cases.append(
            {
                "provider": "lmstudio",
                "base_url": args.local_base_url.rstrip("/"),
                "model": args.local_model,
                "api_key": "",
            }
        )
    return cases


def run_case(case: Dict[str, Any], prompt_id: str, prompt: str, timeout_seconds: int) -> Dict[str, Any]:
    headers = {"Content-Type": "application/json"}
    token = str(case.get("api_key", "")).strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    extra = case.get("extra_headers", {})
    if isinstance(extra, dict):
        headers.update({str(k): str(v) for k, v in extra.items()})

    payload = {
        "model": case["model"],
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.2,
        "max_completion_tokens": 220,
        "top_p": 1,
        "stream": False,
    }

    started = time.time()
    status = None
    text = ""
    usage: Dict[str, Any] = {}
    error = ""
    try:
        url = case["base_url"].rstrip("/") + "/chat/completions"
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout_seconds)
        status = resp.status_code
        parsed = resp.json() if resp.text.strip() else {}
        if isinstance(parsed, dict):
            usage = parsed.get("usage", {}) if isinstance(parsed.get("usage"), dict) else {}
            try:
                content = parsed["choices"][0]["message"]["content"]
                if isinstance(content, str):
                    text = content.strip()
                else:
                    text = str(content).strip()
            except Exception:
                error = str(parsed.get("error") or parsed)[:300]
        else:
            error = "non_json_response"
    except Exception as exc:
        error = str(exc)[:300]

    elapsed_ms = int((time.time() - started) * 1000)
    extraction = extraction_metrics(text) if prompt_id == "work_memory_extract" and text else {}
    return {
        "provider": case["provider"],
        "model": case["model"],
        "prompt_id": prompt_id,
        "status": status,
        "elapsed_ms": elapsed_ms,
        "ok": bool(status == 200 and len(text) > 0),
        "response_chars": len(text),
        "preview": text[:220],
        "usage": usage,
        "error": error,
        "extraction": extraction,
    }


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


def _extract_sections(text: str) -> Dict[str, List[str]]:
    sections: Dict[str, List[str]] = {"next_steps": [], "memory_updates": [], "open_questions": []}
    current = ""
    heading_map = {
        "next steps": "next_steps",
        "next step": "next_steps",
        "memory updates": "memory_updates",
        "memory update": "memory_updates",
        "open questions": "open_questions",
        "open question": "open_questions",
    }
    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue
        m = re.match(r"^(?:#{1,6}\s*)?([A-Za-z ]+):?\s*$", line)
        if m:
            key = heading_map.get(_normalize_line(m.group(1)))
            if key:
                current = key
                continue
        if current:
            bullet = re.sub(r"^\s*(?:[-*]|\d+\.)\s*", "", line).strip()
            if bullet:
                sections[current].append(bullet)
    return sections


def extraction_metrics(text: str) -> Dict[str, Any]:
    sections = _extract_sections(text)
    next_steps = sections.get("next_steps", [])
    memory_updates = sections.get("memory_updates", [])
    open_questions = sections.get("open_questions", [])
    noisy = sum(1 for item in memory_updates if _is_noisy_memory_item(item))
    memory_noise_ratio = (noisy / len(memory_updates)) if memory_updates else 1.0
    coverage = 0.0
    if next_steps:
        coverage += 0.45
    if memory_updates:
        coverage += 0.45
    if open_questions:
        coverage += 0.10
    score = max(0.0, coverage * (1.0 - memory_noise_ratio))
    return {
        "next_steps_count": len(next_steps),
        "memory_updates_count": len(memory_updates),
        "open_questions_count": len(open_questions),
        "memory_update_noise_ratio": round(memory_noise_ratio, 4),
        "extraction_quality_score": round(score, 4),
    }


def _parse_jsonl_last(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        last = ""
        with path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line:
                    last = line
        if not last:
            return None
        parsed = json.loads(last)
        return parsed if isinstance(parsed, dict) else None
    except Exception:
        return None


def summarize(results: List[Dict[str, Any]], weights: Dict[str, float]) -> Dict[str, Any]:
    by_model: Dict[str, Dict[str, Any]] = {}
    for item in results:
        key = f"{item['provider']}::{item['model']}"
        entry = by_model.setdefault(
            key,
            {
                "provider": item["provider"],
                "model": item["model"],
                "runs": 0,
                "oks": 0,
                "latencies": [],
                "response_chars": [],
                "extraction_scores": [],
                "memory_noise_ratios": [],
            },
        )
        entry["runs"] += 1
        if item.get("ok"):
            entry["oks"] += 1
        if isinstance(item.get("elapsed_ms"), int):
            entry["latencies"].append(item["elapsed_ms"])
        if isinstance(item.get("response_chars"), int):
            entry["response_chars"].append(item["response_chars"])
        extraction = item.get("extraction", {})
        if isinstance(extraction, dict):
            score = extraction.get("extraction_quality_score")
            noise = extraction.get("memory_update_noise_ratio")
            if isinstance(score, (int, float)):
                entry["extraction_scores"].append(float(score))
            if isinstance(noise, (int, float)):
                entry["memory_noise_ratios"].append(float(noise))
    summary_rows = []
    min_latency = min([int(sum(e["latencies"]) / max(1, len(e["latencies"]))) for e in by_model.values()] or [1])
    target_response_chars = 180
    for entry in by_model.values():
        lat = entry["latencies"] or [0]
        avg_latency = int(sum(lat) / max(1, len(lat)))
        avg_response_chars = int(sum(entry["response_chars"]) / max(1, len(entry["response_chars"])))
        pass_rate = entry["oks"] / max(1, entry["runs"])
        latency_score = min(1.0, float(min_latency) / max(1.0, float(avg_latency)))
        response_quality_score = min(1.0, float(avg_response_chars) / float(target_response_chars))
        avg_extraction_quality_score = (
            float(sum(entry["extraction_scores"])) / max(1, len(entry["extraction_scores"]))
            if entry["extraction_scores"]
            else 0.0
        )
        avg_memory_noise_ratio = (
            float(sum(entry["memory_noise_ratios"])) / max(1, len(entry["memory_noise_ratios"]))
            if entry["memory_noise_ratios"]
            else 1.0
        )
        weighted_score = (
            (weights["pass_rate"] * pass_rate)
            + (weights["latency"] * latency_score)
            + (weights["response_quality"] * response_quality_score)
            + (weights["extraction_quality"] * avg_extraction_quality_score)
        )
        summary_rows.append(
            {
                "provider": entry["provider"],
                "model": entry["model"],
                "pass_rate": pass_rate,
                "avg_latency_ms": avg_latency,
                "avg_response_chars": avg_response_chars,
                "latency_score": round(latency_score, 4),
                "response_quality_score": round(response_quality_score, 4),
                "avg_extraction_quality_score": round(avg_extraction_quality_score, 4),
                "avg_memory_update_noise_ratio": round(avg_memory_noise_ratio, 4),
                "weighted_score": round(weighted_score, 4),
                "runs": entry["runs"],
            }
        )
    summary_rows.sort(key=lambda x: (x["weighted_score"], x["pass_rate"], -x["avg_latency_ms"]), reverse=True)
    leader = summary_rows[0] if summary_rows else {}
    return {
        "rows": summary_rows,
        "weights": weights,
        "leader": {"provider": leader.get("provider", ""), "model": leader.get("model", ""), "weighted_score": leader.get("weighted_score", 0.0)},
    }


def build_regression_alerts(
    current_rows: List[Dict[str, Any]],
    previous_rows: List[Dict[str, Any]],
    score_drop_threshold: float,
    pass_rate_drop_threshold: float,
    latency_increase_threshold_ms: int,
    extraction_drop_threshold: float,
) -> List[Dict[str, Any]]:
    prev_index = {
        f"{str(r.get('provider', ''))}::{str(r.get('model', ''))}": r for r in previous_rows if isinstance(r, dict)
    }
    alerts: List[Dict[str, Any]] = []
    for row in current_rows:
        key = f"{row.get('provider', '')}::{row.get('model', '')}"
        prev = prev_index.get(key)
        if not prev:
            continue
        score_drop = float(prev.get("weighted_score", 0.0)) - float(row.get("weighted_score", 0.0))
        pass_rate_drop = float(prev.get("pass_rate", 0.0)) - float(row.get("pass_rate", 0.0))
        latency_increase = int(row.get("avg_latency_ms", 0)) - int(prev.get("avg_latency_ms", 0))
        extraction_drop = float(prev.get("avg_extraction_quality_score", 0.0)) - float(
            row.get("avg_extraction_quality_score", 0.0)
        )
        reasons: List[str] = []
        if score_drop >= score_drop_threshold:
            reasons.append(f"weighted_score_drop={round(score_drop, 4)}")
        if pass_rate_drop >= pass_rate_drop_threshold:
            reasons.append(f"pass_rate_drop={round(pass_rate_drop, 4)}")
        if latency_increase >= latency_increase_threshold_ms:
            reasons.append(f"latency_increase_ms={latency_increase}")
        if extraction_drop >= extraction_drop_threshold:
            reasons.append(f"extraction_quality_drop={round(extraction_drop, 4)}")
        if reasons:
            alerts.append(
                {
                    "provider": row.get("provider", ""),
                    "model": row.get("model", ""),
                    "severity": "high" if score_drop >= (score_drop_threshold * 1.5) else "medium",
                    "reasons": reasons,
                    "current": {
                        "weighted_score": row.get("weighted_score", 0.0),
                        "pass_rate": row.get("pass_rate", 0.0),
                        "avg_latency_ms": row.get("avg_latency_ms", 0),
                        "avg_extraction_quality_score": row.get("avg_extraction_quality_score", 0.0),
                    },
                    "previous": {
                        "weighted_score": prev.get("weighted_score", 0.0),
                        "pass_rate": prev.get("pass_rate", 0.0),
                        "avg_latency_ms": prev.get("avg_latency_ms", 0),
                        "avg_extraction_quality_score": prev.get("avg_extraction_quality_score", 0.0),
                    },
                }
            )
    return alerts


def main() -> int:
    parser = argparse.ArgumentParser(description="Benchmark OpenAI/OpenRouter/LM Studio models and persist rolling results")
    parser.add_argument("--out-dir", default="artifacts/benchmarks", help="Output directory for benchmark artifacts")
    parser.add_argument("--openai-model", default="gpt-5-chat-latest")
    parser.add_argument("--openrouter-model", default="openai/gpt-5-chat")
    parser.add_argument("--local-model", default="qwen/qwen3-coder-next")
    parser.add_argument("--local-base-url", default="http://10.0.0.11:1234/v1")
    parser.add_argument("--include-local", action="store_true", help="Include local LM Studio benchmark case")
    parser.add_argument("--openai-key-file", default="/home/joe/codex/openai_api_key.txt")
    parser.add_argument("--openrouter-key-file", default="/home/joe/codex/openrouter_api_key.txt")
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--weight-pass-rate", type=float, default=0.5)
    parser.add_argument("--weight-latency", type=float, default=0.2)
    parser.add_argument("--weight-response-quality", type=float, default=0.15)
    parser.add_argument("--weight-extraction-quality", type=float, default=0.15)
    parser.add_argument("--regression-threshold-score-drop", type=float, default=0.08)
    parser.add_argument("--regression-threshold-pass-rate-drop", type=float, default=0.10)
    parser.add_argument("--regression-threshold-latency-increase-ms", type=int, default=1200)
    parser.add_argument("--regression-threshold-extraction-drop", type=float, default=0.2)
    args = parser.parse_args()

    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    cases = build_cases(args)
    results: List[Dict[str, Any]] = []
    for case in cases:
        if case["provider"] in {"openai", "openrouter"} and not case.get("api_key"):
            results.append(
                {
                    "provider": case["provider"],
                    "model": case["model"],
                    "prompt_id": "setup",
                    "status": None,
                    "elapsed_ms": 0,
                    "ok": False,
                    "response_chars": 0,
                    "preview": "",
                    "usage": {},
                    "error": "missing_api_key",
                }
            )
            continue
        for prompt_id, prompt in default_prompts():
            results.append(run_case(case, prompt_id, prompt, args.timeout_seconds))

    raw_weights = {
        "pass_rate": max(0.0, args.weight_pass_rate),
        "latency": max(0.0, args.weight_latency),
        "response_quality": max(0.0, args.weight_response_quality),
        "extraction_quality": max(0.0, args.weight_extraction_quality),
    }
    weight_sum = sum(raw_weights.values()) or 1.0
    weights = {k: (v / weight_sum) for k, v in raw_weights.items()}

    jsonl_file = out_dir / "matrix_history.jsonl"
    previous_snapshot = _parse_jsonl_last(jsonl_file)
    summary = summarize(results, weights)
    previous_rows = []
    if isinstance(previous_snapshot, dict):
        prev_summary = previous_snapshot.get("summary", {})
        if isinstance(prev_summary, dict):
            rows = prev_summary.get("rows", [])
            if isinstance(rows, list):
                previous_rows = rows
    alerts = build_regression_alerts(
        current_rows=summary.get("rows", []),
        previous_rows=previous_rows,
        score_drop_threshold=max(0.0, args.regression_threshold_score_drop),
        pass_rate_drop_threshold=max(0.0, args.regression_threshold_pass_rate_drop),
        latency_increase_threshold_ms=max(0, args.regression_threshold_latency_increase_ms),
        extraction_drop_threshold=max(0.0, args.regression_threshold_extraction_drop),
    )
    summary["alerts"] = alerts
    snapshot = {"ts": utc_now(), "results": results, "summary": summary}
    latest_file = out_dir / "latest_matrix.json"
    latest_file.write_text(json.dumps(snapshot, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    with jsonl_file.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(snapshot, ensure_ascii=False) + "\n")

    print(json.dumps(snapshot["summary"], ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
