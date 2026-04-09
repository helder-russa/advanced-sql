-- Conservative safe-retrieval view for approved chunks
-- tag::safe_retrieval_view[]
CREATE OR REPLACE VIEW
  `oreilly_support.kb_embeddings_approved_safe` AS
SELECT *
FROM `oreilly_support.kb_article_embeddings`
WHERE is_approved = TRUE
  AND NOT REGEXP_CONTAINS(
    LOWER(content),
    CONCAT(
      r'(ignore\s+previous\s+instructions|system\s+prompt|',
      r'exfiltrate|password|api\s*key)'
    )
  );
-- end::safe_retrieval_view[]

