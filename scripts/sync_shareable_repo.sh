#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHARE_BUILD_DIR="${SHARE_BUILD_DIR:-$ROOT_DIR/artifacts/share/.build-shareable}"
SHARE_REPO_DIR="${SHARE_REPO_DIR:-$ROOT_DIR/artifacts/share/model-cluster-gateway-shareable}"
REMOTE_URL="${SHARE_REMOTE_URL:-https://github.com/josephflowers-ra/model-cluster-gateway.git}"
BRANCH="${SHARE_BRANCH:-main}"
COMMIT_MSG="${SHARE_COMMIT_MSG:-chore(shareable): sync from core repo $(date -u +%Y-%m-%dT%H:%M:%SZ)}"

cd "$ROOT_DIR"
python3 scripts/export_shareable_bundle.py --mvp-pollinations-only --out-dir "$SHARE_BUILD_DIR"

if [[ ! -d "$SHARE_REPO_DIR/.git" ]]; then
  rm -rf "$SHARE_REPO_DIR"
  git clone --branch "$BRANCH" "$REMOTE_URL" "$SHARE_REPO_DIR"
fi

cd "$SHARE_REPO_DIR"
git remote set-url origin "$REMOTE_URL"
git fetch origin "$BRANCH"
git checkout "$BRANCH"
git pull --ff-only origin "$BRANCH"

if command -v rsync >/dev/null 2>&1; then
  rsync -a --delete --exclude '.git/' "$SHARE_BUILD_DIR"/ "$SHARE_REPO_DIR"/
else
  find "$SHARE_REPO_DIR" -mindepth 1 -maxdepth 1 ! -name '.git' -exec rm -rf {} +
  cp -a "$SHARE_BUILD_DIR"/. "$SHARE_REPO_DIR"/
fi

find . -type d -name '__pycache__' -prune -exec rm -rf {} + || true

git add -A
if git diff --cached --quiet; then
  echo "No shareable changes to publish."
  exit 0
fi

git commit -m "$COMMIT_MSG"
git push -u origin "$BRANCH"

echo "Published shareable bundle to $REMOTE_URL ($BRANCH)."
