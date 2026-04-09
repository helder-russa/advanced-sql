-- Identify changed articles and build a chunk delta
-- tag::delta_chunks[]
CREATE TEMP TABLE kb_article_chunks_delta AS
WITH source_articles AS (
  SELECT article_id, body, language, updated_at
  FROM `oreilly_support.support_kb_articles`
  WHERE language = 'en'
    AND updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
)
SELECT
  article_id,
  CONCAT(article_id, ':', LPAD(CAST(offset AS STRING), 4, '0')) AS chunk_id,
  TRIM(paragraph) AS content,
  updated_at
FROM source_articles,
  UNNEST(SPLIT(body, '\n\n')) AS paragraph WITH OFFSET
WHERE TRIM(paragraph) != '';
-- end::delta_chunks[]

-- Embed only the changed chunks
-- tag::delta_embed[]
CREATE TEMP TABLE kb_embeddings_delta AS
SELECT
  d.article_id,
  d.chunk_id,
  d.content,
  d.updated_at,
  ml_generate_embedding_result AS embedding
FROM ML.GENERATE_EMBEDDING(
  MODEL `oreilly_support.text_embedding`,
  (SELECT article_id, chunk_id, content, updated_at FROM kb_article_chunks_delta)
) AS d;
-- end::delta_embed[]

-- Merge delta embeddings into the main table
-- tag::delta_merge[]
MERGE `oreilly_support.kb_article_embeddings` t
USING kb_embeddings_delta s
ON t.chunk_id = s.chunk_id
WHEN MATCHED THEN
  UPDATE SET
    embedding = s.embedding,
    content = s.content,
    updated_at = s.updated_at,
    embedded_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (article_id, chunk_id, content, updated_at, embedding, embedded_at)
  VALUES (s.article_id, s.chunk_id, s.content, s.updated_at, s.embedding, CURRENT_TIMESTAMP());
-- end::delta_merge[]

