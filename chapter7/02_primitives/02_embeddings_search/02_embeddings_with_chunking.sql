-- Chunking knowledge base articles into stable chunks (prototype)
-- tag::kb_embedding_chunking[]
CREATE TEMP TABLE kb_article_chunks AS
WITH source_articles AS (
  SELECT article_id, body, language, updated_at
  FROM `oreilly_support.support_kb_articles`
  WHERE language = 'en'
)
SELECT
  article_id,
  language,
  updated_at,
  offset AS chunk_index,
  CONCAT(article_id, ':', LPAD(CAST(offset AS STRING), 4, '0')) AS chunk_id,
  TRIM(paragraph) AS chunk_text,
  TO_HEX(MD5(TRIM(paragraph))) AS chunk_hash
FROM source_articles,
  UNNEST(SPLIT(body, '\n\n')) AS paragraph WITH OFFSET
WHERE TRIM(paragraph) != '';
-- end::kb_embedding_chunking[]

-- Embedding KB chunks and materializing `kb_article_embeddings` (prototype full refresh)
-- tag::kb_embedding_refresh[]
CREATE OR REPLACE TABLE `oreilly_support.kb_article_embeddings`
CLUSTER BY article_id, embedding_model AS
SELECT
  article_id,
  chunk_id,
  chunk_index,
  content,
  chunk_hash,
  language,
  updated_at,
  ml_generate_embedding_result AS embedding,
  'text-embedding-004' AS embedding_model,
  CURRENT_TIMESTAMP() AS embedded_at
FROM ML.GENERATE_EMBEDDING(
  MODEL `oreilly_support.text_embedding`,
  (SELECT
    article_id,
    chunk_id,
    chunk_index,
    chunk_text as content,
    chunk_hash,
    language,
    updated_at
  FROM kb_article_chunks)
);
-- end::kb_embedding_refresh[]

