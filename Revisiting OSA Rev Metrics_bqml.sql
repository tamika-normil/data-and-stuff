#linear regression 
CREATE OR REPLACE MODEL `etsy-data-warehouse-dev.tnormil.chargeability_model_linear`
OPTIONS
  ( MODEL_TYPE='LINEAR_REG',
    LS_INIT_LEARN_RATE=0.15,
    L1_REG=0,
    MAX_ITERATIONS=5,
    CATEGORY_ENCODING_METHOD='DUMMY_ENCODING', 
    CALCULATE_P_VALUES = TRUE ) AS
SELECT * EXCEPT (order_date,	order_year, order_week_num, order_dayofyear,chargeability, chargeable_aov_dly_yoy, order_year_2022_01_01), chargeability AS LABEL
FROM
  `etsy-data-warehouse-dev.tnormil.chargeability_forecast`;

SELECT
  *
FROM
  ML.ADVANCED_WEIGHTS(MODEL `etsy-data-warehouse-dev.tnormil.chargeability_model_linear`,
    STRUCT(true AS standardize));

#decision trees for feature importance 
CREATE OR REPLACE MODEL `etsy-data-warehouse-dev.tnormil.chargeability_model`
OPTIONS(MODEL_TYPE='BOOSTED_TREE_REGRESSOR',
        BOOSTER_TYPE = 'GBTREE',
        NUM_PARALLEL_TREE = 1,
        MAX_ITERATIONS = 5,
        L2_REG = 0,
        TREE_METHOD = 'HIST',
        EARLY_STOP = TRUE,
        SUBSAMPLE = 0.85)
AS SELECT * EXCEPT (order_date,	order_year, order_week_num, order_dayofyear,chargeability, chargeable_aov_dly_yoy, order_year_2022_01_01), chargeability AS LABEL
FROM
  `etsy-data-warehouse-dev.tnormil.chargeability_forecast`;

SELECT
  *
FROM
  ML.FEATURE_IMPORTANCE(MODEL `etsy-data-warehouse-dev.tnormil.chargeability_model`);
