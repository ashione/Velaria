#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TS_SOURCE="${1:-${PROJECT_ROOT}/src/dataflow/runner/dashboard/app.ts}"
TS_OUTPUT="${2:-${PROJECT_ROOT}/src/dataflow/runner/dashboard/app.js}"
NPM_CACHE_DIR="${NPM_CONFIG_CACHE:-${TMPDIR:-/tmp}/cpp-dataflow-dashboard-npm-cache}"

mkdir -p "${NPM_CACHE_DIR}"
export NPM_CONFIG_CACHE="${NPM_CACHE_DIR}"
export npm_config_cache="${NPM_CACHE_DIR}"

if [[ ! -f "${TS_SOURCE}" ]]; then
  echo "[dashboard-ts] source not found: ${TS_SOURCE}" >&2
  exit 1
fi

if [[ -f "${TS_OUTPUT}" && "${TS_OUTPUT}" -nt "${TS_SOURCE}" ]]; then
  echo "[dashboard-ts] up to date: ${TS_OUTPUT}"
  exit 0
fi

if command -v tsc >/dev/null 2>&1; then
  TS_COMPILER="tsc"
elif node -e "require('typescript/package.json');" >/dev/null 2>&1; then
  TS_COMPILER="node -e 'const ts = require(\"typescript\"); const fs=require(\"fs\"); const p=process.argv[1]; const out=process.argv[2]; const txt=fs.readFileSync(p,'\''utf8'\''); const r=ts.transpileModule(txt,{compilerOptions:{target:ts.ScriptTarget.ES2018,module:ts.ModuleKind.None}}); fs.writeFileSync(out,r.outputText);'"
else
  TS_COMPILER="npx --yes --package=typescript@5.4.5 tsc"
fi

if [[ "$(dirname "${TS_OUTPUT}")" != "." && ! -d "$(dirname "${TS_OUTPUT}")" ]]; then
  mkdir -p "$(dirname "${TS_OUTPUT}")"
fi

echo "[dashboard-ts] compiling ${TS_SOURCE}"
if [[ "${TS_COMPILER}" == tsc ]]; then
  tsc --pretty false --target ES2018 --module none --outFile "${TS_OUTPUT}" "${TS_SOURCE}"
elif [[ "${TS_COMPILER}" == node* ]]; then
  eval ${TS_COMPILER} "${TS_SOURCE}" "${TS_OUTPUT}"
else
  ${TS_COMPILER} --pretty false --target ES2018 --module none --outFile "${TS_OUTPUT}" "${TS_SOURCE}"
fi

echo "[dashboard-ts] generated ${TS_OUTPUT}"
