# Finance Research Candidate Monitoring Verification

Status: local-verified

## Commands

| Command | Result | Evidence |
|---|---|---|
| `uv run --project python --extra finance python -m unittest python.tests.test_finance_pack.FinancePackTest.test_parse_google_news_rss_maps_news_rows_and_sentiment python.tests.test_finance_pack.FinancePackTest.test_rank_candidates_cli_outputs_research_candidates_with_news` after RED tests | failed as expected | `ImportError: cannot import name 'evaluate_news_sentiment'` |
| `uv run --project python --extra finance python -m unittest python.tests.test_finance_pack.FinancePackTest.test_provider_registry_exposes_capabilities_and_catalog python.tests.test_finance_pack.FinancePackTest.test_parse_google_news_rss_maps_news_rows_and_sentiment python.tests.test_finance_pack.FinancePackTest.test_rank_candidates_cli_outputs_research_candidates_with_news` | pass | 3 tests OK |
| `uv run --project python --extra finance python -m unittest python.tests.test_finance_pack` | pass | 20 tests OK |
| `uv run --project python --extra finance python python/velaria_cli.py finance fetch-news --provider google-news --market us --symbol AAPL --limit 3 --preview-rows 2` | pass | real Google News RSS returned 3 AAPL rows |
| `uv run --project python --extra finance python python/velaria_cli.py finance fetch-news --provider google-news --market us --symbol AAPL --limit 2 --preview-rows 1` after HTML cleanup | pass | summary text was cleaned of HTML tags |
| `VELARIA_HOME=/tmp/velaria-rank-hF2bZ2 uv run --project python --extra finance python python/velaria_cli.py finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --top 3 --news-limit 2 --iterations 1 --interval-sec 0 --format json` | pass | returned Top 3 research candidates with quote/history/news/sentiment evidence |
| `VELARIA_HOME=/tmp/velaria-rank-3TgraI uv run --project python --extra finance python python/velaria_cli.py finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --top 3 --news-limit 1 --iterations 1 --interval-sec 0 --format json` | pass | returned top symbols `NVDA,AAPL,MSFT`, event_time populated, top quote freshness delayed |
| service compatibility script using `VelariaService` and `/tmp/velaria-rank-3TgraI` | pass | service saw `finance_us_rank_candidates` source |
| `uv run --project python --extra finance python -m unittest python.tests.test_finance_pack python.tests.test_ai_runtime_agent.AiRuntimeAgentTest.test_cli_run_tool_can_invoke_finance_commands python.tests.test_agentic_service` | pass | 28 tests OK |
| `uv run --project python --extra finance python -m py_compile python/velaria/finance_pack/__init__.py python/velaria/finance_pack/providers.py python/velaria/finance_pack/cli.py python/velaria/cli/finance.py python/tests/test_finance_pack.py` | pass | exit 0 |
| `uv run --project python --extra finance python python/examples/finance_public_data_smoke.py` | pass | real public Yahoo/Tencent smoke returned CN history 18 rows, CN quote 1 row, US history 20 rows, US quote 1 row |
| `uv run --project python --extra finance python python/velaria_cli.py finance fetch-news --provider google-news --market us --symbol AAPL --limit 3 --preview-rows 2` | pass | real Google News RSS returned 3 AAPL rows and cleaned summaries |
| `VELARIA_HOME=/tmp/velaria-rank-live-rT7VXC uv run --project python --extra finance python python/velaria_cli.py finance rank-candidates --market us --symbols AAPL,MSFT,NVDA --start-date 20260501 --end-date 20260518 --top 3 --news-limit 2 --iterations 2 --interval-sec 1 --jsonl` | pass | emitted 2 JSONL ticks; each tick returned 3 `research_candidate` rows with quote/history/news/sentiment evidence |
| first service visibility helper using `list_external_events` | failed, fixed in validation script | helper used a nonexistent method; root cause was script error, not product behavior |
| `PYTHONPATH=python VELARIA_HOME=/tmp/velaria-rank-live-rT7VXC uv run --project python --extra finance python - <<'PY' ... read_external_events(...) ... PY` | pass | `source_seen=true`, `event_count=6`, `symbols=AAPL,MSFT,NVDA`, `recommendation_types=research_candidate` |
| `bazel test --cache_test_results=no //:python_ecosystem_regression` | pass | 14/14 Bazel Python ecosystem tests passed |
| `uv run --project python --extra finance python python/velaria_cli.py finance --help` | pass | help lists `rank-candidates`, `fetch-news`, agent-mode examples, and research-only disclaimer |
| `uv run --project python --extra finance python python/velaria_cli.py finance rank-candidates --help` | pass | help documents history, quote, news providers, continuous iterations, JSONL, and output format |
| `uv run --project python --extra finance python python/velaria_cli.py finance fetch-news --help` | pass | help documents Google News provider, query override, output, and preview rows |
| `git diff --check` | pass | no whitespace or patch format findings |
| focused `rg` scan for common secret/token/private-key patterns across changed files | pass | no matches |

## Gate Ledger

| Gate | Phase | Required Evidence | Status | Evidence | Exception |
|---|---|---|---|---|---|
| G3 Local Development | implementation | suitable worktree and dirty-state protection | pass | existing worktree `feature/finance-agentic-pack`; status clean before edits | |
| G4 Implementation | implementation | changed files map to requirements | pass | provider, CLI, tests, docs, delivery artifacts | |
| G5 Verification | verification | local checks, diff review, secret scan | pass | command table above; diff reviewed; no secret scan matches | |
