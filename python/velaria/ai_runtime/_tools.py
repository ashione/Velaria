"""Velaria tools exposed to AI agents for data analysis."""
from __future__ import annotations

import json
from typing import Any

VELARIA_SQL_SYSTEM_PROMPT = """You are a data analysis assistant for Velaria, a local-first C++20 dataflow engine.

Available SQL (Velaria SQL v1):
- SELECT with projection, aliases, WHERE (AND/OR), GROUP BY, ORDER BY, LIMIT
- Minimal JOIN, UNION / UNION ALL
- INSERT INTO ... VALUES, INSERT INTO ... SELECT
- Builtins: LOWER, UPPER, TRIM, LTRIM, RTRIM, LENGTH, LEN, CHAR_LENGTH, CHARACTER_LENGTH, REVERSE, CONCAT, CONCAT_WS, LEFT, RIGHT, SUBSTR/SUBSTRING, POSITION, REPLACE, ABS, CEIL, FLOOR, ROUND, YEAR, MONTH, DAY

NOT supported: CTE, subqueries, HAVING, WINDOW functions, stored procedures

You have access to Velaria tools to execute SQL, read files, inspect schemas, and explain queries.
Use these tools to analyze data. Return results and explanations to the user."""

VELARIA_TOOLS = [
    {
        "name": "velaria_sql",
        "description": "Execute a SQL query on the current dataset and return results as a list of row dicts",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The SQL query to execute"},
            },
            "required": ["query"],
        },
    },
    {
        "name": "velaria_read",
        "description": "Read a file and return its schema and preview rows",
        "input_schema": {
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "File path to read"},
                "limit": {"type": "integer", "description": "Max rows to return", "default": 20},
            },
            "required": ["path"],
        },
    },
    {
        "name": "velaria_schema",
        "description": "Get the current dataset's column names",
        "input_schema": {
            "type": "object",
            "properties": {},
        },
    },
    {
        "name": "velaria_explain",
        "description": "Get the execution plan for a SQL query without executing it",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "The SQL query to explain"},
            },
            "required": ["query"],
        },
    },
]


def execute_tool(tool_name: str, args: dict, *, session: Any, dataset_context: dict) -> dict:
    """Execute a Velaria tool and return the result."""
    if tool_name == "velaria_sql":
        query = args["query"]
        table_name = dataset_context.get("table_name", "input_table")
        # Ensure the dataset is registered
        if dataset_context.get("source_path") and not dataset_context.get("_registered"):
            df = session.read(dataset_context["source_path"])
            session.create_temp_view(table_name, df)
            dataset_context["_registered"] = True
        result_df = session.sql(query)
        table = result_df.to_arrow()
        rows = table.slice(0, min(table.num_rows, 50)).to_pylist()
        return {"schema": table.schema.names, "rows": rows, "row_count": table.num_rows}

    if tool_name == "velaria_read":
        path = args["path"]
        limit = args.get("limit", 20)
        df = session.read(path)
        table = df.to_arrow()
        rows = table.slice(0, min(table.num_rows, limit)).to_pylist()
        return {"schema": table.schema.names, "rows": rows, "row_count": table.num_rows}

    if tool_name == "velaria_schema":
        schema = dataset_context.get("schema", [])
        return {"schema": schema, "table_name": dataset_context.get("table_name", "input_table")}

    if tool_name == "velaria_explain":
        query = args["query"]
        explain = session.explain_sql(query) if hasattr(session, "explain_sql") else ""
        return {"explain": explain}

    return {"error": f"unknown tool: {tool_name}"}
