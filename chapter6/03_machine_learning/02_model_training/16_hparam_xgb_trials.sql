-- Inspect trials and the chosen hyperparameters
-- tag::hparam_xgb_trials[]
SELECT *
FROM ML.TRIAL_INFO(MODEL `oreilly.usertype_xgb_tuned`)
ORDER BY training_loss asc
LIMIT 20;
-- end::hparam_xgb_trials[]

