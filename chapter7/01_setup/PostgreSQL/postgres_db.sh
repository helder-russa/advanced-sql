docker run --name local-postgres \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=app \
  -p 5432:5432 \
  -v postgres_data:/var/lib/postgresql/data \
  -v "$PWD/initdb":/docker-entrypoint-initdb.d \
  -d postgres:16