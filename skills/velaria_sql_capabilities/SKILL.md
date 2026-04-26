---
name: velaria-sql-capabilities
description: Velaria SQL v1 syntax, functions, and boundaries. What SQL works in the Velaria local analytics engine, what does not, and how to write queries.
---

# Velaria SQL Capabilities

Velaria uses PostgreSQL-compatible parsing (libpg_query frontend) for familiar SQL syntax, but execution is limited to the Velaria local analytics subset.

**Key rule: libpg_query parse success does not mean Velaria support.** Unsupported features are rejected with structured `unsupported_sql_feature` diagnostics.

## SQL Frontend Modes

| Frontend | Env | Description |
|----------|-----|-------------|
| `legacy` | default | Hand-written parser |
| `pg_query` | `VELARIA_SQL_FRONTEND=pg_query` | libpg_query-based, PG syntax compatible |
| `dual` | `VELARIA_SQL_FRONTEND=dual` | Dual-run: legacy executes, pg_query compares |

## Supported SELECT Syntax

```sql
SELECT [col | func(col) AS alias | *]
FROM table [alias]
[WHERE pred [AND|OR pred]]
[GROUP BY col [, col]]
[ORDER BY col [ASC|DESC]]
[LIMIT n]
```

**Predicates**: `=`, `!=`, `<>`, `<`, `<=`, `>`, `>=`, `AND`, `OR`, `NOT`

## Aggregate Functions

`COUNT(*)`, `COUNT(col)`, `SUM`, `MIN`, `MAX`, `AVG`

## Scalar Functions

**String**: `LOWER`, `UPPER`, `TRIM`, `LTRIM`, `RTRIM`, `LENGTH`/`LEN`/`CHAR_LENGTH`, `REVERSE`, `CONCAT`, `CONCAT_WS`, `LEFT`, `RIGHT`, `SUBSTR`/`SUBSTRING`, `POSITION`, `REPLACE`

**Numeric**: `ABS`, `CEIL`, `FLOOR`, `ROUND`

**DateTime**: `YEAR`, `MONTH`, `DAY`, `ISO_YEAR`, `ISO_WEEK`, `WEEK`, `YEARWEEK`, `NOW`, `TODAY`, `CURRENT_TIMESTAMP`, `UNIX_TIMESTAMP`

**Cast**: `CAST(expr AS type)` where type is `STRING`, `INT`, `DOUBLE`, `BOOL`

Functions can be nested: `SUBSTR(CAST(col AS STRING), 1, 6)`

## DDL / DML (legacy frontend only)

```sql
CREATE TABLE t (col1 STRING, col2 INT);
CREATE SOURCE TABLE src USING csv OPTIONS(path:'/data.csv');
CREATE SINK TABLE sink (col1 STRING, col2 INT);
INSERT INTO t VALUES ('a', 1);
INSERT INTO t SELECT col1, col2 FROM other;
```

Constraints: SOURCE TABLE is read-only. SINK TABLE cannot be queried as input.

## Stream SQL

```sql
INSERT INTO output_sink
SELECT region, SUM(amount) AS total
FROM input_stream
WHERE amount > 0
GROUP BY region;
```

Only `INSERT INTO <sink> SELECT ...` via `session.startStreamSql()`.

## Unsupported PostgreSQL Features

| Feature | Status | Workaround |
|---------|--------|------------|
| CTE / WITH | Rejected | Materialize as temp view, query separately |
| Subquery | Rejected | Same as CTE |
| Window functions (batch) | Rejected | Use stream SQL WINDOW BY |
| UNION / INTERSECT / EXCEPT | Rejected | Merge in client |
| DISTINCT ON | Rejected | Use GROUP BY |
| DML/DDL via pg_query | Rejected | Use legacy frontend |
| Multi-statement | Rejected | Execute separately |
| LATERAL JOIN | Rejected | Redesign query |

## Identifier Rules

- Quoted identifiers (`"col"`): exact match only
- Unquoted: exact → case-insensitive → normalized match
- Unicode column names: normalize via `velaria_dataset_normalize` first

## Agent Quick Reference

Parse SQL: `session.sql(sql_str)`
Explain plan: `session.explainSql(sql_str)`
Check frontend: `session.sqlFrontendName()`

Error types from structured diagnostics: `parse_error`, `unsupported_sql_feature`, `bind_error`, `execution_error`

Always call `velaria_sql_capabilities` or `velaria_sql_function_search` for up-to-date function catalog rather than guessing.
