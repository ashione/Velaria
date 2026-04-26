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

## Agent Runtime

Codex runtime dependencies are part of the default Python package. Install
Claude runtime support only when using Claude Code:

```bash
uv sync --project python --extra ai-claude
```

Configure the Agent provider. Both runtimes share the same `agent*` config keys
and the same `~/.velaria/config.json` file.

**Codex runtime** (default):

```bash
mkdir -p ~/.velaria
cat > ~/.velaria/config.json << 'EOF'
{
  "agentProvider": "openai",
  "agentAuthMode": "local",
  "agentRuntime": "codex",
  "agentModel": "gpt-5.4-mini",
  "agentReasoningEffort": "none",
  "agentRuntimeWorkspace": "~/.velaria/ai-runtime",
  "agentReuseLocalConfig": true,
  "agentCodexNetworkAccess": true,
  "agentProxy": "http://127.0.0.1:7897",
  "agentAllProxy": "socks5://127.0.0.1:7897"
}
EOF
```

**Claude runtime** (requires `--extra ai-claude`):

```bash
cat > ~/.velaria/config.json << 'EOF'
{
  "agentProvider": "anthropic",
  "agentAuthMode": "api_key",
  "agentRuntime": "claude",
  "agentModel": "claude-sonnet-4-20250514",
  "agentReasoningEffort": "none",
  "agentApiKey": "<your-api-key>",
  "agentBaseUrl": "<your-api-base-url>",
  "agentRuntimeWorkspace": "~/.velaria/ai-runtime",
  "agentNetworkAccess": true,
  "agentProxy": "http://127.0.0.1:7897"
}
EOF
```

`agentRuntimePath` is optional. Codex can omit it and use the local
`codex app-server` command; set `agentRuntimePath` / `agentCodexRuntimePath` only when
overriding that executable. Claude Code runtime can use `agentClaudeRuntimePath`.
`agentRuntimeWorkspace` is the runtime working directory used for
agent threads, generated config, and MCP/function logs. If omitted, Velaria uses
a project-scoped directory under `~/.velaria/ai-runtime/`. `agentReuseLocalConfig`
controls whether the runtime process can reuse the current user config; set it
to `false` when the runtime should use an isolated HOME. Network access is
controlled by `agentCodexNetworkAccess` (Codex) or `agentNetworkAccess` (Claude),
both defaulting to `true`. `agentProxy` sets both `http_proxy` and
`https_proxy` for the runtime process; use `agentHttpProxy`, `agentHttpsProxy`,
`agentAllProxy`, and `agentNoProxy` for separate values. Shell proxy variables are
also inherited, and Velaria keeps localhost bypassed for local MCP/data URLs.

Model defaults: Codex = `gpt-5.4-mini`, Claude = `claude-sonnet-4-20250514`.
Both runtimes support `agentReasoningEffort` (default `none`) and API key auth
mode (`agentAuthMode: "api_key"` with `agentApiKey`/`agentBaseUrl`).

Use non-interactive SQL generation from the legacy compatibility command:

```bash
uv run --project python python python/velaria_cli.py ai generate-sql \
  --prompt "top 5 by score" --schema "name,score,region"
```

Interactive mode:

```bash
uv run --project python python python/velaria_cli.py -i
› 找出分数最高的5个人
› /status
› :run list --limit 5
```

The interactive CLI is an agent runtime wrapper. It starts a configured
Codex/Claude runtime directly, exposes the Velaria usage skill and SQL catalog
on demand through MCP resources/tools, and registers Velaria local functions
through the runtime bridge / MCP server. `velaria_service`
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
