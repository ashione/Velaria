#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${1:-${ROOT_DIR}/out/sidecar/macos}"
SERVICE_NAME="${2:-velaria-service}"
BUILD_DIR="${OUT_DIR}/.build"

mkdir -p "${OUT_DIR}" "${BUILD_DIR}"

echo "[sidecar] building native extension and native wheel"
bazel build //:velaria_pyext //python:velaria_native_whl

BAZEL_BIN="$(bazel info bazel-bin)"
OUTPUT_BASE="$(bazel info output_base)"
NATIVE_WHL="$(find "${BAZEL_BIN}/python" -maxdepth 1 -name 'velaria-*-native.whl' | head -n1)"
if [[ -z "${NATIVE_WHL}" || ! -f "${NATIVE_WHL}" ]]; then
  echo "[sidecar] native wheel not found under ${BAZEL_BIN}/python" >&2
  exit 1
fi
CPPJIEBA_DICT_DIR="${OUTPUT_BASE}/external/+http_archive+cppjieba_src/dict"
if [[ ! -d "${CPPJIEBA_DICT_DIR}" ]]; then
  echo "[sidecar] cppjieba dict directory not found under ${CPPJIEBA_DICT_DIR}" >&2
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

JIEBA_DIR="${STAGE_DIR}/jieba_dict"
mkdir -p "${JIEBA_DIR}"
echo "[sidecar] copying jieba dictionaries from Bazel external repo"
cp "${CPPJIEBA_DICT_DIR}/jieba.dict.utf8" "${JIEBA_DIR}/jieba.dict.utf8"
cp "${CPPJIEBA_DICT_DIR}/hmm_model.utf8" "${JIEBA_DIR}/hmm_model.utf8"
cp "${CPPJIEBA_DICT_DIR}/user.dict.utf8" "${JIEBA_DIR}/user.dict.utf8"
cp "${CPPJIEBA_DICT_DIR}/idf.utf8" "${JIEBA_DIR}/idf.utf8"
cp "${CPPJIEBA_DICT_DIR}/stop_words.utf8" "${JIEBA_DIR}/stop_words.utf8"

VELARIA_PKG_DIR="$("${PYTHON_BIN}" - <<'PY'
import pathlib
import velaria
print(pathlib.Path(velaria.__file__).resolve().parent)
PY
)"
NATIVE_EXT="${VELARIA_PKG_DIR}/_velaria.so"
if [[ ! -f "${NATIVE_EXT}" ]]; then
  echo "[sidecar] native extension not found in installed wheel: ${NATIVE_EXT}" >&2
  exit 1
fi

echo "[sidecar] packaging velaria_service with PyInstaller"
"${PYTHON_BIN}" -m PyInstaller \
  --noconfirm \
  --clean \
  --onedir \
  --name "${SERVICE_NAME}" \
  --distpath "${OUT_DIR}" \
  --workpath "${BUILD_DIR}/work" \
  --specpath "${BUILD_DIR}/spec" \
  --hidden-import velaria._velaria \
  --add-binary "${NATIVE_EXT}:velaria" \
  --add-data "${JIEBA_DIR}:velaria/jieba_dict" \
  --collect-submodules velaria \
  --collect-submodules velaria_service \
  --collect-binaries velaria \
  --collect-data velaria \
  "${ROOT_DIR}/python/velaria_service/__init__.py"

echo "[sidecar] done: ${OUT_DIR}/${SERVICE_NAME}"
