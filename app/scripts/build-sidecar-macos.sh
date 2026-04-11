#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${1:-${ROOT_DIR}/out/sidecar/macos}"
SERVICE_NAME="${2:-velaria-service}"
BUILD_DIR="${OUT_DIR}/.build"

mkdir -p "${OUT_DIR}" "${BUILD_DIR}"

echo "[sidecar] building native extension and native wheel"
bazel build //:velaria_pyext //python_api:velaria_native_whl

BAZEL_BIN="$(bazel info bazel-bin)"
NATIVE_WHL="$(find "${BAZEL_BIN}/python_api" -maxdepth 1 -name 'velaria-*-native.whl' | head -n1)"
if [[ -z "${NATIVE_WHL}" || ! -f "${NATIVE_WHL}" ]]; then
  echo "[sidecar] native wheel not found under ${BAZEL_BIN}/python_api" >&2
  exit 1
fi

STAGE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/velaria-sidecar.XXXXXX")"
cleanup() {
  rm -rf "${STAGE_DIR}"
}
trap cleanup EXIT

echo "[sidecar] staging venv in ${STAGE_DIR}"
uv venv "${STAGE_DIR}/venv"
PYTHON_BIN="${STAGE_DIR}/venv/bin/python"
NORMALIZED_WHL="$(python3 "${ROOT_DIR}/scripts/normalize_wheel_filename.py" "${NATIVE_WHL}")"
uv pip install --python "${PYTHON_BIN}" "${NORMALIZED_WHL}" pyinstaller

echo "[sidecar] packaging velaria_service.py with PyInstaller"
"${PYTHON_BIN}" -m PyInstaller \
  --noconfirm \
  --clean \
  --onedir \
  --name "${SERVICE_NAME}" \
  --distpath "${OUT_DIR}" \
  --workpath "${BUILD_DIR}/work" \
  --specpath "${BUILD_DIR}/spec" \
  --collect-submodules velaria \
  --collect-binaries velaria \
  --collect-data velaria \
  "${ROOT_DIR}/python_api/velaria_service.py"

echo "[sidecar] done: ${OUT_DIR}/${SERVICE_NAME}"
