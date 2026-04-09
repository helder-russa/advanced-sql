-- Different evaluation metrics computation for model evaluation
-- tag::eval_metrics[]
SELECT *
FROM ML.EVALUATE(MODEL `oreilly.usertype_forest`) 
-- FROM ML.ROC_CURVE(MODEL `oreilly.usertype_forest`)
-- FROM ML.CONFUSION_MATRIX(MODEL `oreilly.usertype_forest`);
-- end::eval_metrics[]

