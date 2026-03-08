#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _load_json(path: Path) -> Dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise RuntimeError(f"Expected JSON object at {path}")
    return data


def _dump_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _sanitize_config_for_share(config: Dict[str, Any], mvp_pollinations_only: bool) -> Dict[str, Any]:
    out = json.loads(json.dumps(config, ensure_ascii=False))

    # Remove file-based key references and inline secrets from named providers.
    backends = out.get("backends", {})
    if isinstance(backends, dict):
        for _, row in backends.items():
            if not isinstance(row, dict):
                continue
            row.pop("api_key_file", None)
            row.pop("api_key_value", None)
            if mvp_pollinations_only and row.get("kind") == "openai":
                row["enabled"] = False

    # Keep Pollinations available without required API setup.
    pollinations = out.get("pollinations", {})
    if isinstance(pollinations, dict):
        pollinations["base_url"] = str(pollinations.get("base_url", "https://text.pollinations.ai")).strip() or "https://text.pollinations.ai"
        pollinations["route_model"] = str(pollinations.get("route_model", "openai-fast")).strip() or "openai-fast"
        pollinations["model"] = str(pollinations.get("model", "gpt-oss-20b")).strip() or "gpt-oss-20b"
        pollinations.pop("api_key_file", None)

    # For a shareable zero-setup MVP, force Pollinations-first route order.
    if mvp_pollinations_only:
        routing = out.get("routing", {})
        routes = routing.get("routes", []) if isinstance(routing, dict) else []
        if isinstance(routes, list):
            for route in routes:
                if not isinstance(route, dict):
                    continue
                route["order"] = [{"type": "pollinations"}]

    return out


def _write_shareable_notes(out_dir: Path, mvp_pollinations_only: bool) -> None:
    notes = [
        "# Shareable Bundle Notes",
        "",
        "This bundle was sanitized for external sharing:",
        "- Removed file-based API key references from provider configs.",
        "- Excluded runtime artifacts/logs and secret files.",
        "",
        "## Quick Start",
        "1. `python3 -m pip install -r requirements.txt`",
        "2. `python3 scripts/model_cluster_gateway.py --config config/gateway_config.shareable.json --host 0.0.0.0 --port 4010`",
        "3. Open `http://127.0.0.1:4010/-/admin`",
        "",
    ]
    if mvp_pollinations_only:
        notes += [
            "## Pollinations MVP Mode",
            "- `config/gateway_config.shareable.json` is set to Pollinations-only routing for no-key startup.",
            "- To enable other providers, add keys and switch to `config/gateway_config.json` or edit the shareable config.",
            "",
        ]
    notes += [
        "## Security",
        "- Do not commit real API keys or local key files into this bundle.",
        "- Prefer environment variables for keys when enabling providers.",
        "",
    ]
    (out_dir / "SHAREABLE_NOTES.md").write_text("\n".join(notes), encoding="utf-8")


def _write_release_notes(out_dir: Path) -> None:
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    notes = [
        "# Release Notes",
        "",
        "## v0.1.0 - Shareable MVP",
        "",
        f"Date: {stamp}",
        "",
        "Highlights:",
        "- OpenAI-compatible gateway with `/v1/chat/completions`, `/v1/team/chat/completions`, and `/v1/work`.",
        "- Built-in admin portal at `/-/admin` for provider setup and chat preview.",
        "- Pollinations-first shareable config (`config/gateway_config.shareable.json`) for no-key startup.",
        "- Team orchestration with stage tracing, verification levels, and cooldown-aware routing behavior.",
        "",
        "Operational defaults:",
        "- `run_gateway.sh` loads `.env` if present.",
        "- If admin credentials are not set, local defaults are used:",
        "  - `GATEWAY_ADMIN_USER=admin`",
        "  - `GATEWAY_ADMIN_PASS=adminpass`",
        "",
        "Upgrade notes:",
        "- For production, set explicit admin credentials in env and rotate them.",
        "- Enable additional providers by configuring API keys and routing in `config/gateway_config.json`.",
        "",
    ]
    (out_dir / "RELEASE_NOTES.md").write_text("\n".join(notes), encoding="utf-8")


def _write_gitignore(out_dir: Path) -> None:
    lines = [
        "__pycache__/",
        "*.pyc",
        "artifacts/",
        "config/secrets/",
    ]
    (out_dir / ".gitignore").write_text("\n".join(lines) + "\n", encoding="utf-8")


def _copy_project_tree(src_root: Path, out_dir: Path) -> None:
    if out_dir.exists():
        shutil.rmtree(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    excludes = {
        "artifacts",
        "__pycache__",
        "meta",
    }
    for path in src_root.iterdir():
        name = path.name
        if name in excludes:
            continue
        if name.startswith(".") and name not in {".env.example"}:
            continue
        target = out_dir / name
        if path.is_dir():
            shutil.copytree(path, target, dirs_exist_ok=True)
        else:
            shutil.copy2(path, target)

    secrets_dir = out_dir / "config" / "secrets"
    if secrets_dir.exists():
        shutil.rmtree(secrets_dir)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export a shareable model-cluster-gateway bundle")
    p.add_argument(
        "--out-dir",
        default="artifacts/share/model-cluster-gateway-shareable",
        help="Output directory for the sanitized bundle (relative to project root by default)",
    )
    p.add_argument(
        "--mvp-pollinations-only",
        action="store_true",
        help="Set shareable config routes to Pollinations-only for no-key startup",
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    src_root = _project_root()
    out_dir = Path(args.out_dir)
    if not out_dir.is_absolute():
        out_dir = src_root / out_dir

    _copy_project_tree(src_root, out_dir)

    src_cfg = src_root / "config" / "gateway_config.json"
    share_cfg = _sanitize_config_for_share(_load_json(src_cfg), bool(args.mvp_pollinations_only))
    _dump_json(out_dir / "config" / "gateway_config.shareable.json", share_cfg)

    _write_shareable_notes(out_dir, bool(args.mvp_pollinations_only))
    _write_release_notes(out_dir)
    _write_gitignore(out_dir)

    summary = {
        "ok": True,
        "out_dir": str(out_dir),
        "shareable_config": str(out_dir / "config" / "gateway_config.shareable.json"),
        "mvp_pollinations_only": bool(args.mvp_pollinations_only),
    }
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
