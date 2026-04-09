-- Export model to Google Cloud Storage
-- tag::export_model[]
EXPORT MODEL `oreilly.usertype_forest`
OPTIONS (uri = 'gs://oreilly-ml-models/usertype_forest/');
-- end::export_model[]

