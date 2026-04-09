-- Hybrid retrieval - stage 1a, normalizing query tokens
-- tag::hybrid_query_tokens[]
CREATE TEMP TABLE query_tokens AS
WITH params AS (
  SELECT 'login timeout after update' AS query_text
)
SELECT token
FROM UNNEST(
  SPLIT(
    REGEXP_REPLACE(
      LOWER((SELECT query_text FROM params)),
      r'[^a-z0-9 ]',
      ' '
    ),
    ' '
  )
) AS token
WHERE token != '' AND LENGTH(token) >= 3;
-- end::hybrid_query_tokens[]

-- Hybrid retrieval - stage 1b, lexical candidates and score
-- tag::hybrid_lexical_candidates[]
CREATE TEMP TABLE lexical_candidates AS
SELECT
  e.article_id,
  e.chunk_id,
  e.content,
  (
    SELECT COUNTIF(
      REGEXP_CONTAINS(
        LOWER(e.content),
        CONCAT(
          r'(^|[^a-z0-9])',
          REGEXP_REPLACE(
            t.token,
            r'([\\.^$|?*+(){}\\[\\]-])',
            r'\\\\\\1'
          ),
          r'([^a-z0-9]|$)'
        )
      )
    )
    FROM query_tokens t
  ) AS lexical_score
FROM `oreilly_support.kb_article_embeddings` e
JOIN `oreilly_support.support_kb_articles` a
  ON a.article_id = e.article_id
WHERE e.language = 'en'
  AND e.embedding_model = 'text-embedding-004'
;
-- end::hybrid_lexical_candidates[]

-- Hybrid retrieval - stage 2a, building the query embedding
-- tag::hybrid_query_embedding[]
CREATE TEMP TABLE query_embedding AS
SELECT ml_generate_embedding_result AS embedding
FROM ML.GENERATE_EMBEDDING(
  MODEL `oreilly_support.text_embedding`,
  (SELECT 'login timeout after update' AS content)
);
-- end::hybrid_query_embedding[]

-- Hybrid retrieval - stage 2b, vector search within candidates
-- tag::hybrid_vector_hits[]
CREATE TEMP TABLE candidate_embeddings AS
SELECT e.chunk_id, e.embedding
FROM `oreilly_support.kb_article_embeddings` e
JOIN lexical_candidates c USING (chunk_id)
WHERE e.embedding_model = 'text-embedding-004'
  AND e.language = 'en';

CREATE TEMP TABLE vector_hits AS
SELECT base.chunk_id, distance
FROM VECTOR_SEARCH(
  TABLE candidate_embeddings,
  'embedding',
  (SELECT embedding FROM query_embedding),
  top_k => 20,
  distance_type => 'COSINE'
);
-- end::hybrid_vector_hits[]

-- Hybrid retrieval - stage 2c, reranking with blended score
-- tag::hybrid_rerank[]
SELECT
  c.article_id,
  c.chunk_id,
  0.7 * (1.0 / (1.0 + v.distance))
    + 0.3 * (LEAST(c.lexical_score, 10) / 10.0) AS hybrid_score,
  c.lexical_score,
  v.distance,
  c.content
FROM vector_hits v
JOIN lexical_candidates c USING (chunk_id)
ORDER BY hybrid_score DESC
LIMIT 5;
-- end::hybrid_rerank[]
