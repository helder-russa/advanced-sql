-- Target Encoding with K-fold Out-of-Fold Mean (Leakage-Aware)
-- tag::target_encoding_oof[]

-- Step A: Build folds and per-(category, fold) aggregates
-- tag::target_encoding_oof_a[]
WITH items AS (
  SELECT
    oi.order_id,
    oi.id,
    oi.sale_price,
    p.category,
    -- Deterministic 0..4 fold id from order_id
    MOD(ABS(FARM_FINGERPRINT(CAST(oi.order_id AS STRING))), 5) AS fold
  FROM `bigquery-public-data.thelook_ecommerce.order_items` AS oi
  JOIN `bigquery-public-data.thelook_ecommerce.products`     AS p  USING (id)
  WHERE oi.sale_price IS NOT NULL
),

-- Step B: per-(category, fold) sums and counts
per_cat_fold AS (
  SELECT
    category,
    fold,
    SUM(sale_price) AS sum_price,
    COUNT(*)        AS n
  FROM items
  GROUP BY category, fold
),
-- end::target_encoding_oof_a[]

-- Step C: global totals per fold and overall (to form an out-of-fold prior)
-- tag::target_encoding_oof_b[]
global_fold AS (
  SELECT
    fold,
    SUM(sale_price) AS sum_fold,
    COUNT(*)        AS n_fold
  FROM items
  GROUP BY fold
),
global_all AS (
  SELECT
    SUM(sale_price) AS sum_all,
    COUNT(*)        AS n_all
  FROM items
),
-- end::target_encoding_oof_b[]

-- Step D: per-category totals across all folds (to subtract current fold)
-- tag::target_encoding_oof_c[]
per_cat_all AS (
  SELECT
    category,
    SUM(sale_price) AS sum_all,
    COUNT(*)        AS n_all
  FROM items
  GROUP BY category
)

-- Step E: leakage-aware encoding from OTHER folds + shrinkage toward an OOF global prior
SELECT
  i.*,
  SAFE_DIVIDE(
    (pc_all.sum_all - COALESCE(pc_fold.sum_price, 0))
    + 20 * SAFE_DIVIDE(
            (glob_all.sum_all - COALESCE(glob_fold.sum_fold, 0)),
            NULLIF(glob_all.n_all - COALESCE(glob_fold.n_fold, 0), 0)
          ),
    (pc_all.n_all - COALESCE(pc_fold.n, 0)) + 20
  ) AS category_target_enc
FROM items AS i
JOIN per_cat_all     AS pc_all   ON pc_all.category = i.category
LEFT JOIN per_cat_fold AS pc_fold ON pc_fold.category = i.category 
  AND pc_fold.fold = i.fold
LEFT JOIN global_fold  AS glob_fold ON glob_fold.fold = i.fold
CROSS JOIN global_all  AS glob_all
LIMIT 1000;
-- end::target_encoding_oof_c[]
-- end::target_encoding_oof[]

