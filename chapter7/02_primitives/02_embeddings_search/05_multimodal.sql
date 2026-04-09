-- Creating a remote embedding model for multimodal embeddings
CREATE OR REPLACE MODEL `oreilly_support.multimodal_embedding`
REMOTE WITH CONNECTION `us.vertex_connection`
OPTIONS(ENDPOINT = 'multimodalembedding@001');


-- Generating embeddings for images
-- tag::multimodal_embedding[]
SELECT
  'img_001' AS image_id,
  ml_generate_embedding_result AS embedding
FROM ML.GENERATE_EMBEDDING(
  MODEL `oreilly_support.multimodal_embedding`,
  (SELECT 'gs://advansed-sql/O_Reilly_Media_logo.svg.png' AS content)
);
-- end::multimodal_embedding[]
