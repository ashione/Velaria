# libpg_query SQL Frontend Spike

## Status: Phase 1 Complete

libpg_query 16-5.1.0 integrated as PostgreSQL-compatible SQL parser frontend.
Parsing, validation, and lowering pipeline works end-to-end for Velaria SQL v1
SELECT queries. Unsupported PG features are rejected with structured diagnostics
and fall back to the legacy hand-written parser.

## Architecture

```
SQL string
  → [VELARIA_SQL_FRONTEND env var: legacy | pg_query | dual]
  → PgQueryFrontend::process()
    ├─ pg_query_parse_protobuf()        # libpg_query C parse
    ├─ PgAstLowerer::lower()            # single-pass validate + lower
    │   ├─ pg_query__parse_result__unpack()  # protobuf-c deserialize
    │   ├─ validateNode()               # policy check during tree walk
    │   └─ lowerSelectStmt()            # build Velaria SqlQuery AST
    └─ → SqlFrontendResult { SqlQuery, diagnostics }
  → SqlPlanner::plan()                  # existing planner (unchanged)
  → Executor::execute()                 # existing executor (unchanged)
```

**Key constraint**: libpg_query strictly isolated to `src/dataflow/core/logical/sql/frontend/`.
PostgreSQL AST never enters optimizer, executor, or public API.

## Module Layout

```
src/dataflow/core/logical/sql/frontend/
  sql_diagnostic.h         # Structured diagnostic with Phase/byte_offset/UTF-16
  source_offset_map.h/.cc  # Byte → line/UTF-8/UTF-16 column mapping
  sql_frontend.h/.cc       # SqlFrontendKind enum + VELARIA_SQL_FRONTEND flag
  sql_feature_validator.h/.cc  # SqlFeaturePolicy + protobuf tree validation
  pg_query_raii.h/.cc      # Opaque RAII wrapper for PgQueryProtobufParseResult
  pg_ast_lowerer.h/.cc     # Protobuf-c tree walk → SqlQuery AST
  pg_query_frontend.h/.cc  # PgQueryFrontend: orchestrate parse→validate→lower
  unbound_plan.h           # UnboundPlan IR (prep for future Binder separation)
```

## Dependencies Added

| Dep | Version | Pinned | Role |
|-----|---------|--------|------|
| libpg_query | 16-5.1.0 | SHA256 | PostgreSQL parser |
| nlohmann/json | 3.11.3 | SHA256 | JSON parsing (reserve for future protobuf→JSON path) |

Both via MODULE.bazel `http_archive` with `build_file_content`.

## Frontend Modes

| Mode | Env | Behavior |
|------|-----|----------|
| `legacy` | default | Hand-written parser (unchanged) |
| `pg_query` | VELARIA_SQL_FRONTEND=pg_query | libpg_query parse → validate → lower → execute. Falls back to legacy if unsupported feature detected |
| `dual` | VELARIA_SQL_FRONTEND=dual | Legacy executes; pg_query runs for comparison and diagnostics |

## Supported SQL (Phase 1)

- SELECT target list: columns, `*`, aliases
- FROM single table with optional alias
- WHERE: AND/OR/NOT, comparison operators (`=`, `!=`, `<`, `>`, `<=`, `>=`)
- GROUP BY simple column refs
- ORDER BY column refs (ASC/DESC)
- LIMIT
- Aggregate: COUNT(*), COUNT(col), SUM, MIN, MAX, AVG
- Scalar functions: LOWER, UPPER, TRIM, LENGTH, CONCAT, SUBSTR, REPLACE, etc.
- CAST
- Literal projection (SELECT 1, 'hello')

## Unsupported Features (Rejected with Structured Diagnostics)

JOIN, subquery, CTE/WITH, window functions, UNION/INTERSECT/EXCEPT, DISTINCT,
DML/DDL, multi-statement, OFFSET, HAVING. Each produces `unsupported_sql_feature`
diagnostic with phase, error_type, message, and hint. Session falls back to
legacy parser automatically.

## Performance (Mac ARM64, 20000 queries)

| Metric | Legacy | pg_query | Ratio |
|--------|--------|----------|-------|
| Parse only | 0.020 ms/q | 0.097 ms/q | 4.9× |
| End-to-end (parse+plan+exec) | 0.059 ms/q | 0.083 ms/q | 1.4× |

Execution hot path (Executor, aggregate, SIMD, filter) is **unaffected** —
the frontend code only runs during SQL parsing.

## Test Coverage

| Test | Coverage |
|------|----------|
| `sql_frontend_test` | SourceOffsetMap, SqlDiagnostic, SqlFrontendConfig, legacy parser compatibility, session integration, EXPLAIN output |
| `sql_feature_validator_test` | SqlFeaturePolicy matrix (agent default, cli default, explicit enable/disable) |
| `sql_dual_frontend_test` | Dual mode, pg_query fallback, frontend name, basic SQL execution |
| `sql_regression_test` | Full v1 regression intact (parser + semantic + planner + batch + stream) |
| `core_regression` | All 11 tests pass (execution not affected) |

## Follow-up

- [ ] Implement JOIN lowering (currently falls back to legacy)
- [ ] Protobuf round-trip elimination (use internal PG node pointers directly)
- [ ] Plan cache keyed by canonical bound plan
- [ ] MCP tools: velaria_sql_to_plan, velaria_plan_explain
- [ ] Upgrade to libpg_query 17.x for native pg_query_parse_json()
