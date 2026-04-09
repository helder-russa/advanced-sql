-- Tokenization with minimal stopword removal
-- tag::tokenization_stopwords[]
WITH base AS (
  SELECT
    id,
    LOWER(REGEXP_REPLACE(title, r'[^a-z0-9 ]', '')) AS clean
  FROM `bigquery-public-data.stackoverflow.posts_questions`
  WHERE title IS NOT NULL
),
tokens AS (
  SELECT id, token
  FROM base, UNNEST(SPLIT(clean, ' ')) AS token
  WHERE token != ''
)
SELECT id, token
FROM tokens
WHERE token NOT IN ('the','a','and','or','to','of','in','on','for','with','is','it','this','that')
LIMIT 100;
-- end::tokenization_stopwords[]

