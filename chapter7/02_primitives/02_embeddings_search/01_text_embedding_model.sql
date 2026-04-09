-- Creating a remote embedding model
-- tag::create_embedding_model[]
CREATE OR REPLACE MODEL `oreilly_support.text_embedding`
REMOTE WITH CONNECTION `us.vertex_connection`
OPTIONS(ENDPOINT = 'text-embedding-004');
-- end::create_embedding_model[]