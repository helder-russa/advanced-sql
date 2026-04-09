# Basic SQL Agent
# Full, runnable example for the minimal agent.

import asyncio
import psycopg
from agents import Agent, Runner, function_tool
from dotenv import load_dotenv

load_dotenv()
DATABASE_URL = "postgresql://postgres:postgres@localhost:5437/app"
MAX_ROWS = 50

# Shared connection (initialized in main)
conn: psycopg.AsyncConnection | None = None


# SQL execution tool for the agent
# tag::sql_tool[]
@function_tool
async def execute_sql(query: str) -> str:
    """Execute a SQL query and return results."""
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
# end::sql_tool[]


# Agent definition and execution loop
# tag::sql_agent[]
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
                "content": "How many open tickets are there per product?",
            }],
        )
        print(result.final_output)


if __name__ == "__main__":
    asyncio.run(main())
# end::sql_agent[]
