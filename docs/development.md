# Development and Verification

This document collects repository-local development, build, smoke, and regression entrypoints.
The root [README.md](../README.md) keeps only the minimal project overview and public-facing paths.

## Python Ecosystem Workflow

Bootstrap:

```bash
bazel build //:velaria_pyext
bazel run //python:sync_native_extension
uv sync --project python --python python3.13
```

Run examples:

```bash
uv run --project python python python/examples/demo_batch_sql_arrow.py
uv run --project python python python/examples/demo_stream_sql.py
uv run --project python python python/examples/demo_vector_search.py
```

Tracked run examples:

```bash
uv run --project python python python/velaria_cli.py -i

uv run --project python python python/velaria_cli.py run start -- file-sql \
  --run-name "score_demo" \
  --description "score filter result for demo input" \
  --tag demo \
  --csv /path/to/input.csv \
  --query "SELECT * FROM input_table LIMIT 5"

uv run --project python python python/velaria_cli.py run list --tag demo --query "score"
uv run --project python python python/velaria_cli.py run result --run-id <run_id>
uv run --project python python python/velaria_cli.py run diff --run-id <run_id> --other-run-id <other_run_id>
uv run --project python python python/velaria_cli.py run show --run-id <run_id>
uv run --project python python python/velaria_cli.py artifacts list --run-id <run_id>
uv run --project python python python/velaria_cli.py artifacts preview --artifact-id <artifact_id>
```

Desktop app prototype:

```bash
cd app
npm install
npm start
```

Build the packaged sidecar:

```bash
bash app/scripts/build-sidecar-macos.sh
```

Build the unsigned local macOS app and `.dmg`:

```bash
bash app/scripts/package-macos.sh
```

Expected outputs:

- `out/sidecar/macos/velaria-service/`
- `out/electron/dist/mac-arm64/Velaria.app`
- `out/electron/dist/Velaria-<version>-arm64.dmg`

Unsigned beta install note on macOS:

- the generated `.dmg` is a beta package when Apple signing and notarization are not configured
- Finder may block the installed app or label it as damaged
- first try `Right Click -> Open` on `Velaria.app`
- if Gatekeeper still blocks it, remove the quarantine attribute:

```bash
xattr -dr com.apple.quarantine /Applications/Velaria.app
```

## Experimental Runtime

Same-host flow:

```text
client -> scheduler(jobmaster) -> worker -> in-proc operator chain -> result
```

## AI Runtime

Bootstrap AI dependencies:

```bash
uv sync --project python --extra ai-claude
# or
uv sync --project python --extra ai-codex
```

Configure AI provider:

```bash
mkdir -p ~/.velaria
cat > ~/.velaria/config.json << 'EOF'
{
  "aiProvider": "claude",
  "aiApiKey": "your-api-key",
  "aiRuntime": "claude",
  "aiModel": "claude-sonnet-4-20250514",
  "aiRuntimePath": "/opt/velaria-runtime/bin/claude",
  "aiRuntimeWorkspace": "~/.velaria/ai-runtime",
  "aiReuseLocalConfig": true,
  "aiCodexNetworkAccess": true,
  "aiProxy": "http://127.0.0.1:7897",
  "aiAllProxy": "socks5://127.0.0.1:7897"
}
EOF
```

`aiRuntimePath` is optional. Codex can omit it and use the local
`codex app-server` command; set `aiRuntimePath` / `aiCodexRuntimePath` only when
overriding that executable. Claude Code runtime can use `aiClaudeRuntimePath` or
`aiRuntimePath`. `aiRuntimeWorkspace` is the runtime working directory used for
agent threads, generated config, and MCP/function logs. If omitted, Velaria uses
a project-scoped directory under `~/.velaria/ai-runtime/`. `aiReuseLocalConfig`
controls whether the runtime process can reuse the current user config; set it
to `false` when the runtime should use an isolated HOME. Codex workspace-write
network access is enabled by default; set `aiCodexNetworkAccess` to `false` only
when the agent runtime must be offline. `aiProxy` sets both `http_proxy` and
`https_proxy` for the runtime process; use `aiHttpProxy`, `aiHttpsProxy`,
`aiAllProxy`, and `aiNoProxy` for separate values. Shell proxy variables are
also inherited, and Velaria keeps localhost bypassed for local MCP/data URLs.

Use AI from the CLI:

```bash
uv run --project python python python/velaria_cli.py ai generate-sql \
  --prompt "top 5 by score" --schema "name,score,region"
```

Interactive mode:

```bash
uv run --project python python python/velaria_cli.py -i
velaria> 找出分数最高的5个人
velaria> /status
velaria> :run list --limit 5
```

The interactive CLI is an agent runtime wrapper. It starts or resumes the
configured Codex/Claude runtime directly, injects the Velaria skill, and exposes
Velaria local functions through the runtime bridge / MCP server. `velaria_service`
remains the HTTP sidecar for the desktop app and other app clients; it is not
required for CLI interactive mode.

Build:

```bash
bazel build //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

Smoke:

```bash
bazel run //:actor_rpc_smoke
```

Three-process local run:

```bash
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

## Build and Verification

Single-node baseline:

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
```

Layered regression entrypoints:

```bash
./scripts/run_core_regression.sh
./scripts/run_python_ecosystem_regression.sh
./scripts/run_experimental_regression.sh
./scripts/run_stream_observability_regression.sh
```

`run_stream_observability_regression.sh` validates the JSON baseline for stream execution, actor strategy/explain output, actor RPC smoke, and the string builtin benchmark cases.

Direct Bazel suites:

```bash
bazel test //:core_regression
bazel test //:python_ecosystem_regression
bazel test //:experimental_regression
```

Release packaging notes:

- Linux release now builds `manylinux x86_64` and `manylinux aarch64` wheels.
- macOS release continues to build `universal2` wheels.
- macOS desktop release also produces a `.dmg`.
- Linux release keeps one wheel per OS/arch and verifies SIMD backend availability from the installed repaired wheel rather than publishing a separate wheel per SIMD instruction set.

Stage benchmark notes:

- `./scripts/run_python_stage_benchmark.sh` defaults to the `groupby_count_max` scenario
- set `VELARIA_STAGE_BENCH_SCENARIO=filter_lower_limit` to validate the `LOWER(method) + filter + LIMIT` path
- set `VELARIA_STAGE_BENCH_QUERY="..."` only for Velaria-only experiments; pair it with
  `VELARIA_STAGE_BENCH_SKIP_HARDCODE=1` when the query no longer matches the selected scenario
- the benchmark wrapper rejects mixed semantics by checking row-count parity before printing
  hardcode-vs-Velaria ratios
