-- Vector search - ticket and query vector
-- tag::vector_search_kb[]
CREATE TEMP TABLE ticket AS
SELECT
  ticket_id,
  product_id,
  CONCAT(subject, '\n', body) AS content
FROM `oreilly_support.support_tickets`
WHERE ticket_id = 'tkt_1002';

CREATE TEMP TABLE query_vec AS
SELECT
  (SELECT ticket_id FROM ticket) AS query_id,
  ml_generate_embedding_result AS embedding
FROM ML.GENERATE_EMBEDDING(
  MODEL `oreilly_support.text_embedding`,
  (SELECT content FROM ticket)
);
-- end::vector_search_kb[]

-- Vector search - retrieve candidate chunks
-- tag::vector_search_retrieved[]
CREATE TEMP TABLE retrieved AS
SELECT
  query.query_id AS query_id,
  base.article_id AS article_id,
  base.chunk_id AS chunk_id,
  base.content AS content,
  distance
FROM VECTOR_SEARCH(
  (SELECT article_id, chunk_id, content, embedding
   FROM `oreilly_support.kb_article_embeddings`
   WHERE embedding_model = 'text-embedding-004'
     AND language = 'en'),
  'embedding',
  (SELECT query_id, embedding FROM query_vec),
  'embedding',
  top_k => 50,
  distance_type => 'COSINE'
);
-- end::vector_search_retrieved[]

-- Vector search - retrieval result table (ready for joins)
-- tag::vector_search_result_table[]
CREATE OR REPLACE TABLE `oreilly_support.ticket_kb_retrieval` AS
WITH params AS (SELECT GENERATE_UUID() AS run_id)
SELECT
  (SELECT run_id FROM params) AS run_id,
  (SELECT ticket_id FROM ticket) AS ticket_id,
  (SELECT product_id FROM ticket) AS product_id,
  r.article_id,
  r.chunk_id,
  r.distance,
  r.content
FROM retrieved r
JOIN `oreilly_support.support_kb_articles` a USING (article_id)
WHERE (SELECT product_id FROM ticket) IN UNNEST(a.product_ids)
ORDER BY r.distance
LIMIT 5;
-- end::vector_search_result_table[]
