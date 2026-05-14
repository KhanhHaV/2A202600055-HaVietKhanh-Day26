import argparse
import json
from fastmcp import FastMCP
from db import SQLiteAdapter, ValidationError
from init_db import create_database

# Initialize database to ensure it exists
db_path = create_database("database.sqlite")
adapter = SQLiteAdapter(db_path)

# Create the server object
mcp = FastMCP("SQLite Lab MCP Server")

@mcp.tool(name="search", description="Search records in a table with optional filtering, ordering, and pagination.")
def search(table: str, filters: dict = None, columns: list = None, limit: int = 20, offset: int = 0, order_by: str = None, descending: bool = False) -> str:
    """
    Search rows in a table.
    Filters should be a dict where keys are column names and values are either the exact match value, or a dict of operators (e.g. {">": 10, "LIKE": "%test%"}).
    """
    try:
        rows = adapter.search(table, columns=columns, filters=filters, limit=limit, offset=offset, order_by=order_by, descending=descending)
        return json.dumps({"status": "success", "rows": rows, "count": len(rows)}, indent=2)
    except ValidationError as e:
        return json.dumps({"status": "error", "message": str(e)}, indent=2)
    except Exception as e:
        return json.dumps({"status": "error", "message": f"Internal error: {str(e)}"}, indent=2)

@mcp.tool(name="insert", description="Insert a new record into a table.")
def insert(table: str, values: dict) -> str:
    """
    Insert a record. Values should be a dictionary mapping column names to values.
    Returns the inserted record.
    """
    try:
        inserted = adapter.insert(table, values)
        return json.dumps({"status": "success", "inserted": inserted}, indent=2)
    except ValidationError as e:
        return json.dumps({"status": "error", "message": str(e)}, indent=2)
    except Exception as e:
        return json.dumps({"status": "error", "message": f"Internal error: {str(e)}"}, indent=2)

@mcp.tool(name="aggregate", description="Compute aggregate metrics (count, sum, avg, min, max) on a table.")
def aggregate(table: str, metric: str, column: str = None, filters: dict = None, group_by: list = None) -> str:
    """
    Aggregate metrics. 
    Metrics allowed: count, sum, avg, min, max.
    Column is required for all metrics except count.
    """
    try:
        rows = adapter.aggregate(table, metric, column=column, filters=filters, group_by=group_by)
        return json.dumps({"status": "success", "rows": rows}, indent=2)
    except ValidationError as e:
        return json.dumps({"status": "error", "message": str(e)}, indent=2)
    except Exception as e:
        return json.dumps({"status": "error", "message": f"Internal error: {str(e)}"}, indent=2)

@mcp.resource("schema://database")
def database_schema() -> str:
    """Returns the full schema of the database."""
    try:
        tables = adapter.list_tables()
        schema = {}
        for table in tables:
            schema[table] = adapter.get_table_schema(table)
        return json.dumps({"status": "success", "schema": schema}, indent=2)
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}, indent=2)

@mcp.resource("schema://table/{table_name}")
def table_schema(table_name: str) -> str:
    """Returns the schema for a specific table."""
    try:
        schema = adapter.get_table_schema(table_name)
        return json.dumps({"status": "success", "table": table_name, "schema": schema}, indent=2)
    except ValidationError as e:
        return json.dumps({"status": "error", "message": str(e)}, indent=2)
    except Exception as e:
        return json.dumps({"status": "error", "message": str(e)}, indent=2)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run FastMCP Database Server")
    parser.add_argument("--transport", type=str, choices=["stdio", "sse"], default="stdio", help="Transport protocol to use (stdio or sse)")
    parser.add_argument("--port", type=int, default=8000, help="Port to use for SSE transport")
    args = parser.parse_args()

    if args.transport == "sse":
        print(f"Starting SSE transport on port {args.port}...")
        mcp.run(transport="sse", port=args.port)
    else:
        mcp.run(transport="stdio")
