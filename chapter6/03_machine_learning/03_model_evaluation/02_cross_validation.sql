-- Cross-validation (complete workflow)
-- tag::cross_validation[]

-- Splitting the dataset into folds
-- tag::cv_preparation[]
CREATE OR REPLACE TABLE `oreilly.taxi_with_folds` AS
SELECT
  CAST(tips > 0 AS INT64) AS tipped,
  trip_seconds,
  trip_miles,
  payment_type,
  EXTRACT(HOUR FROM trip_start_timestamp) AS pickup_hour,
  -- Stable 5-way partition based on an ID
  ABS(MOD(FARM_FINGERPRINT(CAST(taxi_id AS STRING)), 5)) AS fold
FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
WHERE trip_seconds IS NOT NULL
  AND trip_miles  IS NOT NULL
  AND tips        IS NOT NULL
  AND trip_start_timestamp IS NOT NULL
  AND taxi_id     IS NOT NULL;
-- end::cv_preparation[]

-- Step A — Train model on folds ≠ f
-- tag::cv_step_a[]
EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE MODEL `oreilly.taxi_tipped_lr_cv_%d`
  OPTIONS(
    model_type = 'logistic_reg',
    input_label_cols = ['tipped'],
    data_split_method = 'NO_SPLIT'
  ) AS
  SELECT
    tipped,
    trip_seconds,
    trip_miles,
    payment_type,
    pickup_hour
  FROM `oreilly.taxi_with_folds`
  WHERE fold != %d
""", f, f);
-- end::cv_step_a[]

-- Step B — Create results table from evaluation on fold f
-- tag::cv_step_b[]
EXECUTE IMMEDIATE FORMAT("""
  CREATE OR REPLACE TABLE `oreilly.cv_results` AS
  SELECT %d AS fold, *
  FROM ML.EVALUATE(
    MODEL `oreilly.taxi_tipped_lr_cv_%d`,
    (
      SELECT
        tipped,
        trip_seconds,
        trip_miles,
        payment_type,
        pickup_hour
      FROM `oreilly.taxi_with_folds`
      WHERE fold = %d
    )
  )
""", f, f, f);
-- end::cv_step_b[]

-- Step C — Append evaluation metrics for fold f
-- tag::cv_step_c[]
EXECUTE IMMEDIATE FORMAT("""
  INSERT INTO `oreilly.cv_results`
  SELECT %d AS fold, *
  FROM ML.EVALUATE(
    MODEL `oreilly.taxi_tipped_lr_cv_%d`,
    (
      SELECT
        tipped,
        trip_seconds,
        trip_miles,
        payment_type,
        pickup_hour
      FROM `oreilly.taxi_with_folds`
      WHERE fold = %d
    )
  )
""", f, f, f);
-- end::cv_step_c[]

-- Full cross-validation run
-- tag::cv_full[]
DECLARE k INT64 DEFAULT 5;
DECLARE f INT64 DEFAULT 0;

WHILE f < k DO
  -- Step A
  EXECUTE IMMEDIATE FORMAT("...Step A SQL...", f, f);

  IF f = 0 THEN
    -- Step B
    EXECUTE IMMEDIATE FORMAT("...Step B SQL...", f, f, f);
  ELSE
    -- Step C
    EXECUTE IMMEDIATE FORMAT("...Step C SQL...", f, f, f);
  END IF;

  SET f = f + 1;
END WHILE;
-- end::cv_full[]

-- Compute aggregated results of CV
-- tag::cv_results[]
SELECT
  AVG(precision)  AS avg_precision,
  AVG(recall)     AS avg_recall,
  AVG(f1_score)   AS avg_f1,
  AVG(roc_auc)    AS avg_roc_auc,
  STDDEV(roc_auc) AS sd_roc_auc
FROM `oreilly.cv_results`;
-- end::cv_results[]
-- end::cross_validation[]

