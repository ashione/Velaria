# Finance History Subscription Analysis Verification

Status: complete

## Commands

| Command | Result | Evidence |
|---|---|---|
| `uv run --project python --extra finance python -m unittest python.tests.test_finance_pack` after RED tests | failed as expected | `ImportError: cannot import name 'parse_yahoo_chart_payload'` |
| `uv run --project python --extra finance python -m unittest python.tests.test_finance_pack` | pass | 16 tests OK |
| `uv run --project python --extra finance python -m unittest python.tests.test_finance_pack python.tests.test_ai_runtime_agent.AiRuntimeAgentTest.test_cli_run_tool_can_invoke_finance_commands python.tests.test_agentic_service` | pass | 24 tests OK |
| `uv run --project python --extra finance python -m py_compile python/velaria/finance_pack/__init__.py python/velaria/finance_pack/cli.py python/velaria/cli/finance.py python/examples/finance_public_data_smoke.py python/tests/test_finance_pack.py` | pass | exit code 0 |
| `uv run --project python --extra finance python python/velaria_cli.py finance fetch-history --provider yahoo --market cn --symbol 000001 --start-date 20250101 --end-date 20250131 --preview-rows 1` | pass | real Yahoo chart data returned 18 rows |
| `VELARIA_HOME=<tmp> uv run --project python --extra finance python python/velaria_cli.py finance pipeline --market cn --symbol 000001 --start-date 20250101 --end-date 20250131 --iterations 1 --interval-sec 0 --format json` | pass | real pipeline returned history row_count 18 and one FocusEvent |
| `VELARIA_HOME=<tmp> uv run --project python --extra finance python python/velaria_cli.py finance pipeline --market cn --symbol 000001 --start-date 20250101 --end-date 20250131 --iterations 1 --interval-sec 0` | pass | text report rendered |
| service compatibility script using `VelariaService` and same `VELARIA_HOME` | pass | service saw source, monitor, and one focus event |
| `uv run --project python --extra finance python python/examples/finance_public_data_smoke.py` | pass | CN history 18 rows, CN quote 1 row, US history 20 rows, US quote 1 row |
| `bazel test --cache_test_results=no //:python_ecosystem_regression` | pass | 14/14 tests passed |
| `git diff --check` | pass | exit code 0 |
| focused fallback secret scan over changed files | pass | no matches |

## Baseline Findings

- Tencent quote provider worked for `cn:000001`.
- AkShare/Eastmoney historical provider failed in this environment with a structured `provider_fetch_failed` proxy/upstream error.
- Stooq CSV download path requested a manual apikey and is not suitable as a default automated provider.
- Yahoo chart JSON returned historical OHLCV for both `000001.SZ` and `AAPL`.

## Gate Ledger

| Gate | Phase | Required Evidence | Status | Evidence | Exception |
|---|---|---|---|---|---|
| G3 Local Development | implementation | suitable worktree and dirty-state protection | pass | existing clean worktree `feature/finance-agentic-pack` | |
| G4 Implementation | implementation | changed files map to requirements | pass | finance provider, CLI, tests, docs, delivery artifacts | |
| G5 Verification | verification | local checks, diff review, secret scan | pass | command table above | |
