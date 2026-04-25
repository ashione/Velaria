"""On-demand Velaria SQL reference catalog for agent tools and MCP resources."""
from __future__ import annotations

import json
import re
from typing import Any


SQL_CATALOG_URI = "velaria://sql/catalog"


SQL_CAPABILITIES: dict[str, Any] = {
    "version": "sql-v1",
    "clauses": [
        "SELECT projection/aliases",
        "WHERE with literals and column-to-column predicates",
        "GROUP BY columns or supported scalar expressions",
        "ORDER BY output columns",
        "LIMIT",
        "minimal JOIN",
        "INSERT INTO ... VALUES",
        "INSERT INTO ... SELECT",
        "CREATE TABLE",
        "CREATE SOURCE TABLE",
        "CREATE SINK TABLE",
    ],
    "boundaries": [
        "No CTEs",
        "No subqueries",
        "No UNION in stream SQL",
        "No broad ANSI window SQL beyond the existing stream WINDOW BY syntax",
        "SOURCE TABLE is read-only",
        "SINK TABLE can be written but must not be used as a query input",
    ],
    "notes": [
        "Supported scalar functions can be nested in projection expressions.",
        "GROUP BY may use supported scalar expressions directly.",
        "Use SQL-safe ASCII identifiers after dataset normalization when source columns are not SQL identifiers.",
    ],
}


SQL_FUNCTIONS: list[dict[str, Any]] = [
    {
        "name": "LOWER",
        "category": "string",
        "signature": "LOWER(text)",
        "returns": "STRING",
        "description": "Return lowercase text.",
        "examples": ["SELECT LOWER(region) AS region_lower FROM input_table"],
        "keywords": ["string", "lowercase", "case", "normalize text"],
    },
    {
        "name": "UPPER",
        "category": "string",
        "signature": "UPPER(text)",
        "returns": "STRING",
        "description": "Return uppercase text.",
        "examples": ["SELECT UPPER(region) AS region_upper FROM input_table"],
        "keywords": ["string", "uppercase", "case"],
    },
    {
        "name": "TRIM",
        "category": "string",
        "signature": "TRIM(text)",
        "returns": "STRING",
        "description": "Trim leading and trailing whitespace.",
        "examples": ["SELECT TRIM(name) AS name_clean FROM input_table"],
        "keywords": ["string", "whitespace", "clean"],
    },
    {
        "name": "LTRIM",
        "category": "string",
        "signature": "LTRIM(text)",
        "returns": "STRING",
        "description": "Trim leading whitespace.",
        "examples": ["SELECT LTRIM(name) AS name_left_clean FROM input_table"],
        "keywords": ["string", "whitespace", "clean", "left trim"],
    },
    {
        "name": "RTRIM",
        "category": "string",
        "signature": "RTRIM(text)",
        "returns": "STRING",
        "description": "Trim trailing whitespace.",
        "examples": ["SELECT RTRIM(name) AS name_right_clean FROM input_table"],
        "keywords": ["string", "whitespace", "clean", "right trim"],
    },
    {
        "name": "LENGTH",
        "aliases": ["LEN", "CHAR_LENGTH", "CHARACTER_LENGTH"],
        "category": "string",
        "signature": "LENGTH(text)",
        "returns": "INT",
        "description": "Return string length.",
        "examples": ["SELECT LENGTH(TODAY()) AS today_len"],
        "keywords": ["string", "length", "size", "len"],
    },
    {
        "name": "SUBSTR",
        "aliases": ["SUBSTRING"],
        "category": "string",
        "signature": "SUBSTR(text, start_one_based[, length])",
        "returns": "STRING",
        "description": "Return a substring using one-based start positions.",
        "examples": ["SELECT SUBSTR(CAST(open AS STRING), 1, 6) AS open_prefix FROM input_table"],
        "keywords": ["string", "substring", "prefix", "slice"],
    },
    {
        "name": "LEFT",
        "category": "string",
        "signature": "LEFT(text, length)",
        "returns": "STRING",
        "description": "Return the leftmost characters.",
        "examples": ["SELECT LEFT(region, 2) AS region_prefix FROM input_table"],
        "keywords": ["string", "prefix", "left"],
    },
    {
        "name": "RIGHT",
        "category": "string",
        "signature": "RIGHT(text, length)",
        "returns": "STRING",
        "description": "Return the rightmost characters.",
        "examples": ["SELECT RIGHT(code, 3) AS code_suffix FROM input_table"],
        "keywords": ["string", "suffix", "right"],
    },
    {
        "name": "POSITION",
        "category": "string",
        "signature": "POSITION(needle, text)",
        "returns": "INT",
        "description": "Return the one-based position of a substring, or 0 when absent.",
        "examples": ["SELECT POSITION('-', payload) AS dash_pos FROM input_table"],
        "keywords": ["string", "find", "position", "contains"],
    },
    {
        "name": "REPLACE",
        "category": "string",
        "signature": "REPLACE(text, search, replacement)",
        "returns": "STRING",
        "description": "Replace all occurrences of a substring.",
        "examples": ["SELECT REPLACE(region, 'ap', 'AP') AS region_clean FROM input_table"],
        "keywords": ["string", "replace", "clean", "normalize"],
    },
    {
        "name": "REVERSE",
        "category": "string",
        "signature": "REVERSE(text)",
        "returns": "STRING",
        "description": "Reverse text.",
        "examples": ["SELECT REVERSE(code) AS reversed_code FROM input_table"],
        "keywords": ["string", "reverse"],
    },
    {
        "name": "CONCAT",
        "category": "string",
        "signature": "CONCAT(value[, ...])",
        "returns": "STRING",
        "description": "Concatenate values as strings.",
        "examples": ["SELECT CONCAT(region, '-', id) AS region_id FROM input_table"],
        "keywords": ["string", "join text", "combine"],
    },
    {
        "name": "CONCAT_WS",
        "category": "string",
        "signature": "CONCAT_WS(separator, value[, ...])",
        "returns": "STRING",
        "description": "Concatenate values with a separator.",
        "examples": ["SELECT CONCAT_WS('-', region, id) AS region_id FROM input_table"],
        "keywords": ["string", "join text", "combine", "separator"],
    },
    {
        "name": "CAST",
        "category": "conversion",
        "signature": "CAST(value AS STRING|TEXT|INT|INTEGER|INT64|BIGINT|DOUBLE|FLOAT|REAL|BOOL|BOOLEAN)",
        "returns": "requested type",
        "description": "Convert a value to a supported scalar type.",
        "examples": ["SELECT CAST(open_text AS DOUBLE) AS open_value FROM input_table"],
        "keywords": ["cast", "convert", "type", "numeric", "string"],
    },
    {
        "name": "ABS",
        "category": "numeric",
        "signature": "ABS(number)",
        "returns": "DOUBLE",
        "description": "Return absolute value.",
        "examples": ["SELECT ABS(delta) AS abs_delta FROM input_table"],
        "keywords": ["numeric", "absolute", "magnitude"],
    },
    {
        "name": "ROUND",
        "category": "numeric",
        "signature": "ROUND(number)",
        "returns": "DOUBLE",
        "description": "Round a number to the nearest integer value.",
        "examples": ["SELECT ROUND(amount) AS rounded_amount FROM input_table"],
        "keywords": ["numeric", "round", "integer"],
    },
    {
        "name": "CEIL",
        "category": "numeric",
        "signature": "CEIL(number)",
        "returns": "DOUBLE",
        "description": "Return the smallest integer value greater than or equal to the input.",
        "examples": ["SELECT CEIL(amount) AS amount_ceiling FROM input_table"],
        "keywords": ["numeric", "ceil", "ceiling", "round up"],
    },
    {
        "name": "FLOOR",
        "category": "numeric",
        "signature": "FLOOR(number)",
        "returns": "DOUBLE",
        "description": "Return the largest integer value less than or equal to the input.",
        "examples": ["SELECT FLOOR(amount) AS amount_floor FROM input_table"],
        "keywords": ["numeric", "floor", "round down"],
    },
    {
        "name": "YEAR",
        "category": "date",
        "signature": "YEAR(timestamp_or_date)",
        "returns": "INT",
        "description": "Extract UTC calendar year from epoch milliseconds, timestamp, or date text.",
        "examples": ["SELECT YEAR(trade_date) AS year FROM input_table"],
        "keywords": ["date", "time", "year", "extract"],
    },
    {
        "name": "MONTH",
        "category": "date",
        "signature": "MONTH(timestamp_or_date)",
        "returns": "INT",
        "description": "Extract UTC month number.",
        "examples": ["SELECT MONTH(trade_date) AS month FROM input_table"],
        "keywords": ["date", "time", "month", "extract"],
    },
    {
        "name": "DAY",
        "category": "date",
        "signature": "DAY(timestamp_or_date)",
        "returns": "INT",
        "description": "Extract UTC day-of-month.",
        "examples": ["SELECT DAY(trade_date) AS day FROM input_table"],
        "keywords": ["date", "time", "day", "extract"],
    },
    {
        "name": "ISO_WEEK",
        "aliases": ["WEEK"],
        "category": "date",
        "signature": "ISO_WEEK(timestamp_or_date)",
        "returns": "INT",
        "description": "Extract ISO week number. WEEK uses the same ISO week semantics.",
        "examples": ["SELECT ISO_WEEK(trade_date) AS iso_week FROM input_table"],
        "keywords": ["date", "time", "week", "iso", "weekly", "group by week"],
    },
    {
        "name": "ISO_YEAR",
        "category": "date",
        "signature": "ISO_YEAR(timestamp_or_date)",
        "returns": "INT",
        "description": "Extract ISO week-year; use with ISO_WEEK for cross-year weekly grouping.",
        "examples": ["SELECT ISO_YEAR(trade_date) AS iso_year FROM input_table"],
        "keywords": ["date", "time", "iso", "year", "weekly", "cross year"],
    },
    {
        "name": "YEARWEEK",
        "category": "date",
        "signature": "YEARWEEK(timestamp_or_date)",
        "returns": "INT",
        "description": "Return compact ISO year/week key as YYYYWW.",
        "examples": ["SELECT YEARWEEK(trade_date) AS year_week FROM input_table"],
        "keywords": ["date", "time", "yearweek", "weekly", "group by week"],
    },
    {
        "name": "NOW",
        "category": "time",
        "signature": "NOW()",
        "returns": "STRING",
        "description": "Return current UTC timestamp text.",
        "examples": ["SELECT NOW() AS generated_at"],
        "keywords": ["current", "now", "timestamp", "time", "generated at"],
    },
    {
        "name": "CURRENT_TIMESTAMP",
        "aliases": ["currentTimestamp"],
        "category": "time",
        "signature": "CURRENT_TIMESTAMP()",
        "returns": "STRING",
        "description": "Return current UTC timestamp text. currentTimestamp() is accepted as an alias.",
        "examples": ["SELECT currentTimestamp() AS generated_at"],
        "keywords": ["current", "timestamp", "now", "time"],
    },
    {
        "name": "TODAY",
        "category": "time",
        "signature": "TODAY()",
        "returns": "STRING",
        "description": "Return current UTC date text as YYYY-MM-DD.",
        "examples": ["SELECT TODAY() AS run_date"],
        "keywords": ["current", "today", "date", "run date"],
    },
    {
        "name": "UNIX_TIMESTAMP",
        "category": "time",
        "signature": "UNIX_TIMESTAMP([timestamp_or_date])",
        "returns": "INT",
        "description": "Return current Unix seconds, or convert an input timestamp/date to Unix seconds.",
        "examples": [
            "SELECT UNIX_TIMESTAMP() AS generated_epoch_s",
            "SELECT UNIX_TIMESTAMP(trade_date) AS trade_epoch_s FROM input_table",
        ],
        "keywords": ["unix", "epoch", "seconds", "timestamp", "convert date"],
    },
]


SQL_PATTERNS: list[dict[str, Any]] = [
    {
        "name": "weekly_aggregation",
        "category": "date",
        "description": "Aggregate rows by ISO week using ISO_YEAR and ISO_WEEK.",
        "keywords": ["weekly", "week", "按周", "group by week", "weekly aggregate"],
        "template": (
            "SELECT ISO_YEAR({date_col}) AS iso_year, ISO_WEEK({date_col}) AS iso_week, "
            "COUNT(*) AS n, AVG({value_col}) AS avg_value "
            "FROM {table_name} GROUP BY ISO_YEAR({date_col}), ISO_WEEK({date_col}) "
            "ORDER BY iso_year, iso_week"
        ),
        "parameters": ["table_name", "date_col", "value_col"],
        "functions": ["ISO_YEAR", "ISO_WEEK"],
    },
    {
        "name": "yearweek_aggregation",
        "category": "date",
        "description": "Aggregate rows by compact ISO YYYYWW key.",
        "keywords": ["yearweek", "weekly", "week key", "YYYYWW"],
        "template": (
            "SELECT YEARWEEK({date_col}) AS year_week, COUNT(*) AS n, AVG({value_col}) AS avg_value "
            "FROM {table_name} GROUP BY YEARWEEK({date_col}) ORDER BY year_week"
        ),
        "parameters": ["table_name", "date_col", "value_col"],
        "functions": ["YEARWEEK"],
    },
    {
        "name": "current_run_metadata",
        "category": "time",
        "description": "Add current date/time metadata to query output.",
        "keywords": ["current", "today", "now", "timestamp", "metadata", "generated"],
        "template": (
            "SELECT *, TODAY() AS run_date, NOW() AS generated_at, "
            "UNIX_TIMESTAMP() AS generated_epoch_s FROM {table_name}"
        ),
        "parameters": ["table_name"],
        "functions": ["TODAY", "NOW", "UNIX_TIMESTAMP"],
    },
    {
        "name": "date_to_epoch",
        "category": "time",
        "description": "Convert a timestamp/date column to Unix seconds.",
        "keywords": ["unix", "epoch", "timestamp", "convert", "date"],
        "template": "SELECT {date_col}, UNIX_TIMESTAMP({date_col}) AS epoch_s FROM {table_name}",
        "parameters": ["table_name", "date_col"],
        "functions": ["UNIX_TIMESTAMP"],
    },
]


def sql_capabilities(section: str = "") -> dict[str, Any]:
    section = section.strip().lower()
    if section == "functions":
        return {"version": SQL_CAPABILITIES["version"], "functions": _function_summary()}
    if section == "patterns":
        return {"version": SQL_CAPABILITIES["version"], "patterns": SQL_PATTERNS}
    if section in {"clauses", "boundaries", "notes"}:
        return {"version": SQL_CAPABILITIES["version"], section: SQL_CAPABILITIES[section]}
    return {
        **SQL_CAPABILITIES,
        "function_categories": sorted({str(item["category"]) for item in SQL_FUNCTIONS}),
        "function_count": len(SQL_FUNCTIONS),
        "pattern_count": len(SQL_PATTERNS),
        "resource_uri": SQL_CATALOG_URI,
    }


def search_sql_functions(query: str = "", category: str = "", name: str = "", limit: int = 10) -> dict[str, Any]:
    query_terms = _terms(query)
    category = category.strip().lower()
    name_upper = name.strip().upper()
    scored: list[tuple[int, dict[str, Any]]] = []
    for item in SQL_FUNCTIONS:
        score = 0
        names = [str(item["name"]).upper()] + [str(alias).upper() for alias in item.get("aliases", [])]
        if name_upper:
            if name_upper in names:
                score += 100
            else:
                continue
        if category:
            if str(item["category"]).lower() == category:
                score += 20
            else:
                continue
        haystack = _search_text(item)
        if query_terms:
            matched = sum(1 for term in query_terms if term in haystack)
            if matched == 0:
                continue
            score += matched * 10
        scored.append((score, item))
    scored.sort(key=lambda pair: (-pair[0], str(pair[1]["name"])))
    matches = [dict(item) for _, item in scored[: max(1, min(int(limit), 50))]]
    return {
        "query": query,
        "category": category,
        "name": name,
        "matches": matches,
        "count": len(matches),
        "resource_uri": SQL_CATALOG_URI,
    }


def suggest_sql_patterns(
    task: str = "",
    table_name: str = "input_table",
    columns: list[str] | None = None,
    limit: int = 5,
) -> dict[str, Any]:
    query_terms = _terms(task)
    scored: list[tuple[int, dict[str, Any]]] = []
    for item in SQL_PATTERNS:
        haystack = _search_text(item)
        score = sum(1 for term in query_terms if term in haystack) * 10 if query_terms else 1
        if score <= 0:
            continue
        pattern = dict(item)
        pattern["template"] = _fill_pattern_defaults(pattern["template"], table_name, columns or [])
        scored.append((score, pattern))
    scored.sort(key=lambda pair: (-pair[0], str(pair[1]["name"])))
    return {
        "task": task,
        "table_name": table_name,
        "columns": columns or [],
        "patterns": [item for _, item in scored[: max(1, min(int(limit), 20))]],
        "resource_uri": SQL_CATALOG_URI,
    }


def sql_catalog_markdown() -> str:
    lines = [
        "# Velaria SQL Catalog",
        "",
        f"Version: `{SQL_CAPABILITIES['version']}`",
        "",
        "## Capabilities",
    ]
    lines.extend(f"- {item}" for item in SQL_CAPABILITIES["clauses"])
    lines.extend(["", "## Boundaries"])
    lines.extend(f"- {item}" for item in SQL_CAPABILITIES["boundaries"])
    lines.extend(["", "## Scalar Functions"])
    for item in SQL_FUNCTIONS:
        aliases = item.get("aliases") or []
        alias_text = f" aliases: {', '.join(aliases)}" if aliases else ""
        lines.append(f"- `{item['signature']}` ({item['category']}) -> {item['returns']}.{alias_text} {item['description']}")
        for example in item.get("examples", [])[:2]:
            lines.append(f"  - `{example}`")
    lines.extend(["", "## Query Patterns"])
    for item in SQL_PATTERNS:
        lines.append(f"- `{item['name']}`: {item['description']}")
        lines.append(f"  - `{item['template']}`")
    return "\n".join(lines) + "\n"


def sql_catalog_json() -> str:
    return json.dumps(
        {
            "capabilities": SQL_CAPABILITIES,
            "functions": SQL_FUNCTIONS,
            "patterns": SQL_PATTERNS,
            "resource_uri": SQL_CATALOG_URI,
        },
        ensure_ascii=False,
        indent=2,
    )


def _function_summary() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for item in SQL_FUNCTIONS:
        out.setdefault(str(item["category"]), []).append(str(item["name"]))
    return out


def _terms(value: str) -> list[str]:
    terms: list[str] = []
    for part in value.lower().replace("_", " ").split():
        if not part:
            continue
        terms.append(part)
        if re.search(r"[\u4e00-\u9fff]", part) and len(part) > 2:
            terms.extend(part[index : index + 2] for index in range(0, len(part) - 1))
    return terms


def _search_text(item: dict[str, Any]) -> str:
    parts: list[str] = []
    for key in ("name", "category", "signature", "description", "template"):
        if item.get(key):
            parts.append(str(item[key]))
    for key in ("aliases", "keywords", "examples", "functions"):
        values = item.get(key) or []
        if isinstance(values, list):
            parts.extend(str(value) for value in values)
    return " ".join(parts).lower().replace("_", " ")


def _fill_pattern_defaults(template: str, table_name: str, columns: list[str]) -> str:
    date_col = _first_matching_column(columns, ["date", "time", "ts", "timestamp"]) or "date_col"
    value_col = _first_matching_column(
        columns,
        ["amount", "value", "price", "close", "score", "count", "volume"],
    ) or _first_non_date_column(columns, date_col) or "value_col"
    return template.format(
        table_name=table_name or "input_table",
        date_col=date_col,
        value_col=value_col,
    )


def _first_matching_column(columns: list[str], needles: list[str]) -> str:
    for column in columns:
        lower = column.lower()
        if any(needle in lower for needle in needles):
            return column
    return ""


def _first_non_date_column(columns: list[str], date_col: str) -> str:
    for column in columns:
        if column != date_col:
            return column
    return ""
