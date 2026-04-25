#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY_DIR="${ROOT_DIR}/python"
OUT_DIR="${1:-${ROOT_DIR}/dist}"
EXE_NAME="${2:-velaria-cli}"

mkdir -p "${OUT_DIR}"

NATIVE_SO="${ROOT_DIR}/bazel-bin/_velaria.so"
if [[ ! -f "${NATIVE_SO}" ]]; then
  echo "[build-cli] building native extension //:velaria_pyext"
  bazel build //:velaria_pyext
  NATIVE_SO="$(bazel info bazel-bin)/_velaria.so"
fi

if [[ ! -f "${NATIVE_SO}" ]]; then
  echo "[build-cli] native extension not found: ${NATIVE_SO}" >&2
  exit 1
fi

TARGET_SO="${PY_DIR}/velaria/_velaria.so"
if [[ -f "${TARGET_SO}" ]]; then
  chmod u+w "${TARGET_SO}" || true
  rm -f "${TARGET_SO}"
fi
cp "${NATIVE_SO}" "${TARGET_SO}"

cleanup() {
  if [[ -f "${TARGET_SO}" ]]; then
    chmod u+w "${TARGET_SO}" || true
  fi
  rm -f "${TARGET_SO}"
}
trap cleanup EXIT

echo "[build-cli] packaging one-file executable with uv + pyinstaller"
uv run --project "${PY_DIR}" --with pyinstaller pyinstaller \
  --onefile \
  --name "${EXE_NAME}" \
  --distpath "${OUT_DIR}" \
  --workpath "${OUT_DIR}/.build" \
  --specpath "${OUT_DIR}/.spec" \
  "${PY_DIR}/velaria_cli.py"

echo "[build-cli] done: ${OUT_DIR}/${EXE_NAME}"
