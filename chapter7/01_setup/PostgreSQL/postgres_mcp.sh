docker run -p 8000:8000 \
  -e DATABASE_URI=postgresql://postgres:postgres@localhost:5432/app \
  crystaldba/postgres-mcp --access-mode=restricted --transport=sse