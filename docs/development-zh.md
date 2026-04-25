# 开发与验证

这份文档集中放置仓库内的开发、构建、smoke 和回归入口。
根 [README-zh.md](../README-zh.md) 只保留最小项目说明和公开入口。

## Python 生态工作流

初始化：

```bash
bazel build //:velaria_pyext
bazel run //python:sync_native_extension
uv sync --project python --python python3.13
```

运行示例：

```bash
uv run --project python python python/examples/demo_batch_sql_arrow.py
uv run --project python python python/examples/demo_stream_sql.py
uv run --project python python python/examples/demo_vector_search.py
```

tracked run 示例：

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

桌面 app 原型：

```bash
cd app
npm install
npm start
```

构建打包 sidecar：

```bash
bash app/scripts/build-sidecar-macos.sh
```

构建本地未签名 macOS app 与 `.dmg`：

```bash
bash app/scripts/package-macos.sh
```

预期产物：

- `out/sidecar/macos/velaria-service/`
- `out/electron/dist/mac-arm64/Velaria.app`
- `out/electron/dist/Velaria-<version>-arm64.dmg`

macOS 未签名内测包安装说明：

- 当没有配置 Apple 签名和 notarization 时，这里产出的 `.dmg` 只是内测包
- Finder 可能会阻止安装后的 app，或者提示“已损坏”
- 先在 Finder 里对 `Velaria.app` 执行“右键 -> 打开”
- 如果 Gatekeeper 仍然阻止启动，可以手动移除隔离属性：

```bash
xattr -dr com.apple.quarantine /Applications/Velaria.app
```

## Experimental Runtime

同机执行链路：

```text
client -> scheduler(jobmaster) -> worker -> in-proc operator chain -> result
```

## AI Runtime

安装 AI 依赖：

```bash
uv sync --project python --extra ai-claude
# 或
uv sync --project python --extra ai-codex
```

配置 AI provider：

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

`aiRuntimePath` 是可选项。Codex 可以省略它并使用本地 `codex app-server`
命令；只有需要覆盖该可执行文件时才设置 `aiRuntimePath` /
`aiCodexRuntimePath`。Claude Code runtime 可以使用 `aiClaudeRuntimePath` 或
`aiRuntimePath`。`aiRuntimeWorkspace` 是 runtime 工作目录，用于保存 agent
thread、生成配置以及 MCP/function 日志；如果省略，Velaria 会使用
`~/.velaria/ai-runtime/` 下的项目级目录。`aiReuseLocalConfig` 控制 runtime
进程是否复用当前用户配置；设为 `false` 时 runtime 会使用隔离 HOME。
Codex workspace-write 网络访问默认开启；只有需要离线运行 agent runtime
时才把 `aiCodexNetworkAccess` 设为 `false`。`aiProxy` 会同时设置 runtime
进程的 `http_proxy` 和 `https_proxy`；需要分别配置时使用 `aiHttpProxy`、
`aiHttpsProxy`、`aiAllProxy` 和 `aiNoProxy`。Shell 里的代理变量也会被继承，
Velaria 会默认保留 localhost 绕过，以免影响本地 MCP/data URL。

从 CLI 使用 AI：

```bash
uv run --project python python python/velaria_cli.py ai generate-sql \
  --prompt "top 5 by score" --schema "name,score,region"
```

交互模式：

```bash
uv run --project python python python/velaria_cli.py -i
velaria> 找出分数最高的5个人
velaria> /status
velaria> :run list --limit 5
```

交互式 CLI 是 agent runtime wrapper：它会直接启动或恢复已配置的
Codex/Claude runtime，注入 Velaria skill，并通过 runtime bridge / MCP server
暴露 Velaria local functions。`velaria_service` 仍作为桌面 app 和其他 app
client 使用的 HTTP sidecar，交互式 CLI 不需要先启动 service。

构建：

```bash
bazel build //:actor_rpc_scheduler //:actor_rpc_worker //:actor_rpc_client //:actor_rpc_smoke
```

smoke：

```bash
bazel run //:actor_rpc_smoke
```

三进程本地运行：

```bash
bazel run //:actor_rpc_scheduler -- --listen 127.0.0.1:61000 --node-id scheduler
bazel run //:actor_rpc_worker -- --connect 127.0.0.1:61000 --node-id worker-1
bazel run //:actor_rpc_client -- --connect 127.0.0.1:61000 --payload "demo payload"
```

## 构建与验证

单机基线：

```bash
bazel run //:sql_demo
bazel run //:df_demo
bazel run //:stream_demo
```

分层回归入口：

```bash
./scripts/run_core_regression.sh
./scripts/run_python_ecosystem_regression.sh
./scripts/run_experimental_regression.sh
./scripts/run_stream_observability_regression.sh
```

`run_stream_observability_regression.sh` 会校验 stream 执行、actor strategy/explain 输出、actor RPC smoke，以及字符串 builtin benchmark case 的 JSON 基线。

直接使用 Bazel suite：

```bash
bazel test //:core_regression
bazel test //:python_ecosystem_regression
bazel test //:experimental_regression
```

Release 打包说明：

- Linux release 现在会构建：
  - `manylinux x86_64`
  - `manylinux aarch64`
- macOS release 继续构建：
  - `universal2 wheel`
- macOS 桌面 release 还会产出：
  - `.dmg`
- Linux release 会保持“每个 OS/arch 一个 wheel”，不会再按 SIMD 指令集额外拆 wheel；同一 wheel 内部再通过 runtime SIMD dispatch 选择后端。

Stage benchmark 说明：

- `./scripts/run_python_stage_benchmark.sh` 默认跑 `groupby_count_max` 场景
- 需要校验 `LOWER(method) + filter + LIMIT` 路径时，设置
  `VELARIA_STAGE_BENCH_SCENARIO=filter_lower_limit`
- 只有在做 Velaria-only 实验时才设置 `VELARIA_STAGE_BENCH_QUERY="..."`
- 当自定义 query 已经不再匹配所选场景时，必须同时设置
  `VELARIA_STAGE_BENCH_SKIP_HARDCODE=1`
- benchmark wrapper 会先校验 hardcode 与 Velaria 的结果行数一致，再输出 ratio，
  避免混入不同语义的基线
