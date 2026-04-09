-- One-way ANOVA - Complete Analysis
-- tag::anova[]

-- Step 0: Migration of public dataset to local environment (run once)
-- tag::anova_dataset_prep[]
CREATE OR REPLACE TABLE oreilly.natality_sample AS
SELECT
  mother_race,
  weight_pounds,
  year
FROM `bigquery-public-data.samples.natality`
WHERE weight_pounds IS NOT NULL
    AND mother_race IS NOT NULL;
-- end::anova_dataset_prep[]

-- Step 1: Prepare sample data from public natality dataset
-- tag::anova_data_prep[]
WITH birth_data AS (
  SELECT 
    mother_race,
    weight_pounds
  FROM `oreilly.natality_sample`
  WHERE weight_pounds IS NOT NULL 
    AND weight_pounds > 0 
    AND mother_race IN (1, 2, 3)  -- White, Black, American Indian
    AND year = 2008
  LIMIT 1000  -- Keep sample size manageable
),
-- end::anova_data_prep[]

-- Step 2: Map race codes to readable names
-- tag::anova_data_interpretation[]
coded_data AS (
  SELECT 
    CASE mother_race
      WHEN 1 THEN 'White'
      WHEN 2 THEN 'Black' 
      WHEN 3 THEN 'American_Indian'
    END as race_group,
    weight_pounds
  FROM birth_data
),
-- end::anova_data_interpretation[]

-- Step 3: Calculate group statistics
-- tag::anova_descriptive_stats[]
group_stats AS (
  SELECT 
    race_group,
    COUNT(weight_pounds) as n,
    AVG(weight_pounds) as group_mean,
    VAR_POP(weight_pounds) as group_variance
  FROM coded_data
  GROUP BY race_group
),
-- end::anova_descriptive_stats[]

-- Step 4: Calculate overall statistics  
-- tag::anova_overall_stats[]
overall_stats AS (
  SELECT 
    COUNT(weight_pounds) as total_n,
    AVG(weight_pounds) as grand_mean,
    COUNT(DISTINCT race_group) as num_groups
  FROM coded_data
),
-- end::anova_overall_stats[]

-- Step 5: Calculate ANOVA components
-- tag::anova_core_components[]
anova_calc AS (
  SELECT 
    -- Degrees of freedom
    os.num_groups - 1 as df_between,
    os.total_n - os.num_groups as df_within,
    
    -- Sum of Squares Between Groups
    SUM(gs.n * POWER(gs.group_mean - os.grand_mean, 2)) as ssb,
    
    -- Sum of Squares Within Groups  
    SUM((gs.n - 1) * gs.group_variance) as ssw,
    
    -- Mean Squares
    SUM(gs.n * POWER(gs.group_mean - os.grand_mean, 2)) / (os.num_groups - 1) as msb,
    SUM((gs.n - 1) * gs.group_variance) / (os.total_n - os.num_groups) as msw
    
  FROM group_stats gs
  CROSS JOIN overall_stats os
  GROUP BY os.num_groups, os.total_n, os.grand_mean
)
-- end::anova_core_components[]

-- Final Results: Group summaries and ANOVA table
-- tag::anova_final_results[]
SELECT 'GROUP_SUMMARY' as result_type, race_group as category, 
       CAST(n as STRING) as value_1, 
       CAST(ROUND(group_mean, 3) as STRING) as value_2,
       CAST(ROUND(SQRT(group_variance), 3) as STRING) as value_3
FROM group_stats

UNION ALL

SELECT 'ANOVA_TABLE' as result_type, 'Between_Groups' as category,
       CAST(df_between as STRING) as value_1,
       CAST(ROUND(ssb, 2) as STRING) as value_2, 
       CAST(ROUND(msb, 4) as STRING) as value_3
FROM anova_calc

UNION ALL

SELECT 'ANOVA_TABLE', 'Within_Groups',
       CAST(df_within as STRING),
       CAST(ROUND(ssw, 2) as STRING),
       CAST(ROUND(msw, 4) as STRING) 
FROM anova_calc

UNION ALL

SELECT 'ANOVA_TABLE', 'F_Statistic', 
       NULL as value_1,
       NULL as value_2,
       CAST(ROUND(msb/msw, 4) as STRING) as value_3
FROM anova_calc

ORDER BY result_type DESC, value_1 NULLS LAST;
-- end::anova_final_results[]
-- end::anova[]

