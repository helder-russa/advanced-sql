# SQL Agent via MCP
# Uses Model Context Protocol instead of custom tools.

import asyncio

from agents import Agent, Runner
from agents.mcp import MCPServerSse
from dotenv import load_dotenv

load_dotenv()


# Connecting an agent to postgres-mcp
# tag::agent_mcp[]
async def main():
    # Connect to the postgres-mcp server
    async with MCPServerSse(
        params={"url": "http://localhost:8000/sse"},
        name="postgres",
    ) as mcp_server:

        agent = Agent(
            name="sql_agent",
            instructions="""You are a SQL assistant for a PostgreSQL database.
            Use the available tools to explore the schema and run queries.
            Always check the schema before writing queries.""",
            mcp_servers=[mcp_server],
        )

        result = await Runner.run(
            agent,
            input=[{
                "role": "user",
                "content": "What tables are available and how many tickets are open?",
            }],
        )
        print(result.final_output)


if __name__ == "__main__":
    asyncio.run(main())
# end::agent_mcp[]
