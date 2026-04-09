-- Creating a remote model connected to Gemini
-- tag::create_remote_model[]
CREATE OR REPLACE MODEL `oreilly_support.gemini_flash`
REMOTE WITH CONNECTION `us.vertex_connection`
OPTIONS(ENDPOINT = 'gemini-2.5-flash');
-- end::create_remote_model[]