-- Feature hashing (hashing trick)
-- tag::feature_hashing[]
SELECT
id AS question_id,
tags,
ABS(MOD(FARM_FINGERPRINT(tags), 1024)) AS tag_bucket_1024
FROM bigquery-public-data.stackoverflow.posts_questions
WHERE tags IS NOT NULL
LIMIT 100;
-- end::feature_hashing[]

