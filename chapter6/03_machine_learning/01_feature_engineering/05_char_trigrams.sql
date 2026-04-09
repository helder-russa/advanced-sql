-- Character 3-grams (subword features)
-- tag::char_trigrams[]
WITH base AS (
  SELECT
    id,
    LOWER(REGEXP_REPLACE(title, r'[^a-z0-9]', '')) AS t
  FROM `bigquery-public-data.stackoverflow.posts_questions`
  WHERE title IS NOT NULL
)
SELECT
  id,
  SUBSTR(t, pos, 3) AS trigram
FROM base,
UNNEST(GENERATE_ARRAY(1, GREATEST(LENGTH(t) - 2, 0))) AS pos
WHERE LENGTH(t) >= 3
LIMIT 200;
-- end::char_trigrams[]

