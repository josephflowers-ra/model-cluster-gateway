#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Optional


def _extract_assistant_text(body: Dict[str, Any]) -> str:
    choices = body.get("choices", [])
    if isinstance(choices, list) and choices:
        first = choices[0]
        if isinstance(first, dict):
            msg = first.get("message", {})
            if isinstance(msg, dict):
                content = msg.get("content", "")
                if isinstance(content, str):
                    return content.strip()
                if isinstance(content, list):
                    parts: List[str] = []
                    for item in content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            txt = str(item.get("text", "")).strip()
                            if txt:
                                parts.append(txt)
                    return "\n".join(parts).strip()
    result = body.get("result", {})
    if isinstance(result, dict):
        txt = str(result.get("final_answer", "")).strip()
        if txt:
            return txt
    return ""


def _extract_request_messages(body: Dict[str, Any], path: str) -> List[Dict[str, str]]:
    messages = body.get("messages", [])
    if isinstance(messages, list) and messages:
        out: List[Dict[str, str]] = []
        for item in messages:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role", "")).strip()
            content = item.get("content", "")
            if not role:
                continue
            if isinstance(content, str):
                txt = content.strip()
            elif isinstance(content, list):
                parts: List[str] = []
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "text":
                        ptxt = str(part.get("text", "")).strip()
                        if ptxt:
                            parts.append(ptxt)
                txt = "\n".join(parts).strip()
            else:
                txt = str(content).strip()
            if txt:
                out.append({"role": role, "content": txt})
        if out:
            return out

    # Fallback for /v1/work
    if path == "/v1/work":
        objective = str(body.get("objective", "")).strip()
        mode = str(body.get("mode", "research")).strip()
        project_id = str(body.get("project_id", "default-project")).strip()
        if objective:
            return [
                {
                    "role": "user",
                    "content": f"[work:{mode}] project={project_id}\n{objective}",
                }
            ]
    return []


def _read_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except Exception:
            continue
        if isinstance(item, dict):
            rows.append(item)
    return rows


def build_dataset(rows: List[Dict[str, Any]], include_work: bool) -> List[Dict[str, Any]]:
    by_req: Dict[str, Dict[str, Any]] = {}
    for item in rows:
        event_type = str(item.get("event_type", "")).strip()
        payload = item.get("payload", {})
        if not isinstance(payload, dict):
            continue
        request_id = str(payload.get("request_id", "")).strip()
        if not request_id:
            continue
        path = str(payload.get("path", "")).strip()
        if path not in {"/v1/chat/completions", "/v1/team/chat/completions", "/v1/work"}:
            continue
        if path == "/v1/work" and not include_work:
            continue
        bucket = by_req.setdefault(request_id, {"request_body": None, "response_body": None, "path": path, "status": 0})
        if event_type == "http_request_body":
            body = payload.get("body", {})
            if isinstance(body, dict):
                bucket["request_body"] = body
        elif event_type == "http_response_body":
            body = payload.get("body", {})
            if isinstance(body, dict):
                bucket["response_body"] = body
                bucket["status"] = int(payload.get("status", 0) or 0)
        elif event_type == "http_request":
            bucket["path"] = path or bucket.get("path", "")

    out: List[Dict[str, Any]] = []
    for request_id, item in by_req.items():
        req_body = item.get("request_body", {})
        resp_body = item.get("response_body", {})
        if not isinstance(req_body, dict) or not isinstance(resp_body, dict):
            continue
        status = int(item.get("status", 0) or 0)
        if status and status >= 400:
            continue
        path = str(item.get("path", "")).strip()
        messages = _extract_request_messages(req_body, path=path)
        if not messages:
            continue
        assistant = _extract_assistant_text(resp_body)
        if not assistant:
            continue
        dataset_row = {
            "messages": messages + [{"role": "assistant", "content": assistant}],
            "metadata": {
                "request_id": request_id,
                "path": path,
                "status": status,
                "router": {
                    "team": bool((resp_body.get("team", {}) or {}).get("used", False)) if isinstance(resp_body, dict) else False
                },
            },
        }
        tool_trace = resp_body.get("tool_trace", [])
        if isinstance(tool_trace, list) and tool_trace:
            dataset_row["metadata"]["tool_trace"] = tool_trace
        out.append(dataset_row)
    return out


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export chat-training JSONL from gateway structured logs")
    parser.add_argument("--log-file", default="artifacts/logs/gateway_events.jsonl", help="Input gateway events JSONL path")
    parser.add_argument("--out", default="artifacts/datasets/chat_training_from_logs.jsonl", help="Output dataset JSONL path")
    parser.add_argument("--include-work", action="store_true", help="Include /v1/work rows as chat-style user+assistant pairs")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log_path = Path(args.log_file).expanduser().resolve()
    out_path = Path(args.out).expanduser().resolve()
    rows = _read_jsonl(log_path)
    dataset = build_dataset(rows, include_work=bool(args.include_work))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        for item in dataset:
            fh.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(
        json.dumps(
            {
                "ok": True,
                "log_file": str(log_path),
                "out": str(out_path),
                "rows_in": len(rows),
                "rows_out": len(dataset),
                "include_work": bool(args.include_work),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
