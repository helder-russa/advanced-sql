# SQL Agent Guardrails
# Read-only validation, tenant filter enforcement, and a runnable agent.

import asyncio
import pglast
import psycopg
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


# Adding read-only query validation with pglast
# tag::readonly_validation[]
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
# end::readonly_validation[]


# AST-based tenant filter validation
# tag::required_filters[]
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
# end::required_filters[]


# Using the filter checker in the tool
# tag::filter_validation[]
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
# end::filter_validation[]


# SQL execution tool with guardrails
@function_tool
async def execute_sql(query: str, product_id: str) -> str:
    """Execute a SQL query scoped to a specific product."""
    is_safe, reason = is_read_only(query)
    if not is_safe:
        return f"QUERY REJECTED: {reason}\nPlease generate a SELECT query only."
    if not has_required_filter(query, "product_id", product_id):
        return f"Query must filter by product_id = '{product_id}'"

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


# Runnable agent with guardrails
async def main():
    global conn
    async with await psycopg.AsyncConnection.connect(DATABASE_URL) as conn:
        agent = Agent(
            name="sql_agent",
            instructions="""\
You are a SQL assistant for a PostgreSQL database.
Use the oreilly_support schema (tables: support_tickets, product_catalog).
Always filter by product_id. If a query fails, fix it and retry.""",
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
