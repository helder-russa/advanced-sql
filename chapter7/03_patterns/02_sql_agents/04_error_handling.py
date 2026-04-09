# SQL Agent with Guardrails, Schema Context, and Error Handling
# Full, runnable example of the most robust custom-tool approach.

import asyncio
import psycopg
import pglast
from agents import Agent, Runner, function_tool
from dotenv import load_dotenv
from pglast import ast, parse_sql
from pglast.enums import A_Expr_Kind
from pglast.visitors import Visitor

load_dotenv()
DATABASE_URL = "postgresql://postgres:postgres@localhost:5437/app"
MAX_ROWS = 50

# Shared connection (initialized in main)
conn: psycopg.AsyncConnection | None = None


# Read-only validation (same as 2_guardrails.py)
WRITE_NODES = (
    ast.InsertStmt, ast.UpdateStmt, ast.DeleteStmt,
    ast.DropStmt, ast.TruncateStmt,  # extend as needed
)


def is_read_only(sql: str) -> tuple[bool, str]:
    """Check if SQL is read-only. Returns (is_safe, reason)."""
    try:
        for raw_stmt in parse_sql(sql):
            if isinstance(raw_stmt.stmt, WRITE_NODES):
                return False, f"{type(raw_stmt.stmt).__name__} not allowed"
        return True, "OK"
    except pglast.parser.ParseError as e:
        return False, f"Parse error: {e}"


# Required filter validation
class FilterChecker(Visitor):
    """Walk the AST to find equality filters on a specific column."""

    def __init__(self, column: str, value: str):
        self.column, self.value, self.found = column, value, False

    def visit_A_Expr(self, ancestors, node):
        if node.kind == A_Expr_Kind.AEXPR_OP:
            op = node.name[0].sval if node.name else None
            if op == "=" and self._matches(node.lexpr, node.rexpr):
                self.found = True

    def _matches(self, left, right):
        col = val = None
        if isinstance(left, ast.ColumnRef):
            col = left.fields[-1].sval if left.fields else None
        if isinstance(right, ast.A_Const) and hasattr(right.val, "sval"):
            val = right.val.sval
        return col == self.column and val == self.value


def has_required_filter(sql: str, column: str, value: str) -> bool:
    """Check if SQL contains an equality filter on column = value."""
    try:
        tree = parse_sql(sql)
        checker = FilterChecker(column, value)
        checker(tree)
        return checker.found
    except Exception as e:
        print(f"Filter validation error: {e}")
        return False


# Fetching and injecting schema context
async def get_schema_context(conn, schema_name: str = "oreilly_support") -> str:
    """Fetch schema information from PostgreSQL."""
    query = """
    SELECT table_name, column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = %s
    ORDER BY table_name, ordinal_position
    """
    async with conn.cursor() as cur:
        await cur.execute(query, (schema_name,))
        rows = await cur.fetchall()

    # Format as a readable schema description
    tables = {}
    for table, column, dtype, nullable in rows:
        qualified = f"{schema_name}.{table}"
        if qualified not in tables:
            tables[qualified] = []
        null_str = "" if nullable == "YES" else " NOT NULL"
        tables[qualified].append(f"  {column}: {dtype}{null_str}")

    schema_text = "Available tables:\n\n"
    for table, columns in tables.items():
        schema_text += f"{table}:\n" + "\n".join(columns) + "\n\n"
    return schema_text


# Rich error messages for agent recovery
# tag::error_handling[]
@function_tool
async def execute_sql(query: str, product_id: str) -> str:
    """Execute a SQL query scoped to a specific product."""
    is_safe, reason = is_read_only(query)
    if not is_safe:
        return f"QUERY REJECTED: {reason}\nPlease generate a SELECT query only."
    if not has_required_filter(query, "product_id", product_id):
        return f"Query must filter by product_id = '{product_id}'"

    try:
        async with conn.cursor() as cur:
            await cur.execute(query)
            if cur.description is None:
                return "Query executed successfully but returned no rows."
            columns = [desc[0] for desc in cur.description]
            rows = await cur.fetchmany(MAX_ROWS + 1)
            if not rows:
                return "Query executed successfully but returned no rows."
            has_more = len(rows) > MAX_ROWS
            rows = rows[:MAX_ROWS]
            result = " | ".join(columns) + "\n"
            result += "\n".join(" | ".join(str(v) for v in row) for row in rows)
            if has_more:
                result += "\n... (more rows not shown)"
            return result
    except psycopg.errors.UndefinedColumn as e:
        return f"ERROR: Unknown column. {e}\nCheck the schema and try again."
    except psycopg.errors.UndefinedTable as e:
        return (
            f"ERROR: Unknown table. {e}\n"
            "Tables: support_tickets, support_kb_articles, product_catalog"
        )
    except psycopg.errors.SyntaxError as e:
        return f"ERROR: SQL syntax error. {e}\nPlease fix the query syntax."
    except Exception as e:
        return f"ERROR: {type(e).__name__}: {e}"
# end::error_handling[]


# Agent definition and execution loop
# tag::sql_agent[]
async def main():
    global conn
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        schema_context = await get_schema_context(conn)

        agent = Agent(
            name="sql_agent",
            instructions=f"""\
You are a SQL assistant for the oreilly_support database.

DATABASE SCHEMA:
{schema_context}
Generate queries using only the tables/columns above.
Always filter by product_id. If a query fails, read the error and retry.""",
            tools=[execute_sql],
        )

        result = await Runner.run(
            agent,
            input=[{
                "role": "user",
                "content": (
                    "For product_id 'prod_analytics_basic', "
                    "how many open tickets are there?"
                ),
            }],
        )
        print(result.final_output)


if __name__ == "__main__":
    asyncio.run(main())
# end::sql_agent[]
