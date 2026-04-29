# libpg_query SQL Frontend Spike

## Status: Historical

This spike is complete. Velaria SQL is now parsed through the pg_query-only
frontend. The former hand-written SQL parser, `legacy` frontend mode, `dual`
frontend mode, and silent fallback path have been removed.

## Current Architecture

```text
SQL string
  -> Velaria extension preprocessor
     - CREATE SOURCE/SINK TABLE metadata
     - USING ... OPTIONS(...)
     - WINDOW BY ... EVERY ... [SLIDE ...] AS ...
     - KEYWORD SEARCH(...) QUERY ... TOP_K ...
     - HYBRID SEARCH ... QUERY ... METRIC ... TOP_K ... SCORE_THRESHOLD ...
     - reserved Velaria function-name normalization
  -> PgQueryFrontend::process()
     - pg_query_parse_protobuf()
     - SqlFeaturePolicy validation
     - PgAstLowerer::lower()
  -> SqlStatement / SqlQuery
  -> SqlPlanner
  -> Executor / stream runtime
```

`VELARIA_SQL_FRONTEND` is no longer a behavior switch. `DataflowSession`
reports `pg_query`, and parser failures surface as structured SQL diagnostics
instead of falling back to another parser.

## Preserved Contract

The migration did not replace Velaria's SQL AST, planner, or runtime contract.
The pg_query tree is isolated to `src/dataflow/core/logical/sql/frontend/` and is
lowered into the existing `SqlStatement` / `SqlQuery` structures.

Velaria SQL v1 syntax remains the public surface:

- `CREATE TABLE`, `CREATE SOURCE TABLE`, `CREATE SINK TABLE`
- `USING csv|line|json OPTIONS(...)`
- `INSERT INTO ... VALUES`
- `INSERT INTO ... SELECT`
- `SELECT` with current projection, filter, aggregate, join, order, limit, and union subset
- `WINDOW BY ... EVERY ... [SLIDE ...] AS ...`
- `KEYWORD SEARCH(...) QUERY ... TOP_K ...`
- `HYBRID SEARCH ... QUERY ... [METRIC ...] [TOP_K ...] [SCORE_THRESHOLD ...]`

Unsupported PostgreSQL features such as CTEs, subqueries, richer joins, and ANSI
window functions remain outside SQL v1 and are rejected with diagnostics.

## Regression Coverage

- `sql_frontend_test`
- `sql_dual_frontend_test` (kept as a pg-only regression target name)
- `planner_v03_test`
- `sql_regression_test`
- `stream_runtime_test`
- `core_regression`
- `velaria_pyext`
- `python_ecosystem_regression`
