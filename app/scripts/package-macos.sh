#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
APP_DIR="${ROOT_DIR}/app"

echo "[package] building sidecar"
bash "${APP_DIR}/scripts/build-sidecar-macos.sh"

cd "${APP_DIR}"
if [[ ! -d node_modules ]]; then
  echo "[package] installing npm dependencies"
  npm install
fi

echo "[package] building macOS dmg"
npx electron-builder --config electron-builder.yml --mac dmg
