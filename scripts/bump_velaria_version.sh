#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <version>" >&2
  exit 1
fi

VERSION="$1"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_BZL="${ROOT_DIR}/python_api/version.bzl"
VERSION_PY="${ROOT_DIR}/python_api/velaria/_version.py"

if [[ ! "${VERSION}" =~ ^[0-9]+\.[0-9]+\.[0-9]+([A-Za-z0-9._-]+)?$ ]]; then
  echo "invalid version: ${VERSION}" >&2
  exit 1
fi

cat > "${VERSION_BZL}" <<EOF
VELARIA_PY_VERSION = "${VERSION}"
EOF

cat > "${VERSION_PY}" <<EOF
__version__ = "${VERSION}"
EOF

if command -v uv >/dev/null 2>&1; then
  uv lock --project "${ROOT_DIR}/python_api"
fi

echo "updated Velaria Python version to ${VERSION}"
