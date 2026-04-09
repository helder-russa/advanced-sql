-- One-way ANOVA - UDF and Conclusions
-- tag::anova_udf[]

-- Create UDF in JavaScript for F critical value
-- tag::anova_udf_f_critical[]
CREATE OR REPLACE FUNCTION oreilly.f_critical_05(
  df1 INT64, 
  df2 INT64
)
RETURNS FLOAT64
LANGUAGE js AS """
  // Simplified lookup table for common F critical values at α = 0.05
  const f_table = {
    '1,10': 4.96, '1,15': 4.54, '1,20': 4.35, '1,30': 4.17,
    '2,10': 4.10, '2,15': 3.68, '2,20': 3.49, '2,30': 3.32,
    '3,10': 3.71, '3,15': 3.29, '3,20': 3.10, '3,30': 2.92,
    '4,10': 3.48, '4,15': 3.06, '4,20': 2.87, '4,30': 2.69
  };
  
  const key = df1 + ',' + df2;
  
  // Return exact match if available
  if (f_table[key]) return f_table[key];
  
  // Simple approximation for other values
  return 3.0 + (10 / df2) - (0.1 * df1);
""";
-- end::anova_udf_f_critical[]

-- Conclusion of Hypothesis test (requires anova_calc CTE from 6_anova.sql)
-- tag::anova_conclusions[]
SELECT 
  ROUND(msb/msw, 4) AS F_Statistic,
  df_between,
  df_within,
  ROUND(oreilly.f_critical_05(df_between, df_within), 3) AS F_Critical,
  CASE 
    WHEN msb/msw > oreilly.f_critical_05(df_between, df_within)
      THEN 'SIGNIFICANT - Reject H0'
    ELSE 'NOT SIGNIFICANT - Fail to reject H0'
  END AS Conclusion
FROM anova_calc;
-- end::anova_conclusions[]
-- end::anova_udf[]

