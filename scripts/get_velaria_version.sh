#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION_FILE="${ROOT_DIR}/python/velaria/_version.py"

if [[ ! -f "${VERSION_FILE}" ]]; then
  echo "version file not found: ${VERSION_FILE}" >&2
  exit 1
fi

VERSION="$(sed -n 's/^__version__ = "\(.*\)"$/\1/p' "${VERSION_FILE}")"

if [[ -z "${VERSION}" ]]; then
  echo "failed to parse version from ${VERSION_FILE}" >&2
  exit 1
fi

printf '%s\n' "${VERSION}"
