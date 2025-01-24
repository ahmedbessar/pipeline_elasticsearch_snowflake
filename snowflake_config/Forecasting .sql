-- This is your Cortex Project.
-----------------------------------------------------------
-- SETUP
-----------------------------------------------------------
use role ACCOUNTADMIN;
use warehouse COMPUTE_WH;
use database WASEET;
use schema RAW;

-- Inspect the first 10 rows of your training data. This is the data we'll use to create your model.
select * from CLASSIFIED_LOG_REPORT_ELS limit 10;

-- Prepare your training data. Timestamp_ntz is a required format. Also, only include select columns.
CREATE VIEW CLASSIFIED_LOG_REPORT_ELS_v1 AS SELECT
    to_timestamp_ntz(DAY) as DAY_v1,
    CALLANDROIDCOUNT,
    MAINTAXONOMYTITLEENGLISH
FROM CLASSIFIED_LOG_REPORT_ELS;



-----------------------------------------------------------
-- CREATE PREDICTIONS
-----------------------------------------------------------
-- Create your model.
CREATE SNOWFLAKE.ML.FORECAST my_model(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'CLASSIFIED_LOG_REPORT_ELS_v1'),
    SERIES_COLNAME => 'MAINTAXONOMYTITLEENGLISH',
    TIMESTAMP_COLNAME => 'DAY_v1',
    TARGET_COLNAME => 'CALLANDROIDCOUNT',
    CONFIG_OBJECT => { 'ON_ERROR': 'SKIP' }
);

-- Generate predictions and store the results to a table.
BEGIN
    -- This is the step that creates your predictions.
    CALL my_model!FORECAST(
        FORECASTING_PERIODS => 14,
        -- Here we set your prediction interval.
        CONFIG_OBJECT => {'prediction_interval': 0.95}
    );
    -- These steps store your predictions to a table.
    LET x := SQLID;
    CREATE TABLE My_forecasts_2025_01_22 AS SELECT * FROM TABLE(RESULT_SCAN(:x));
END;

-- View your predictions.
SELECT * FROM My_forecasts_2025_01_22;

-- Union your predictions with your historical data, then view the results in a chart.
SELECT MAINTAXONOMYTITLEENGLISH, DAY, CALLANDROIDCOUNT AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
    FROM CLASSIFIED_LOG_REPORT_ELS
UNION ALL
SELECT replace(series, '"', '') as MAINTAXONOMYTITLEENGLISH, ts as DAY, NULL AS actual, forecast, lower_bound, upper_bound
    FROM My_forecasts_2025_01_22;

-----------------------------------------------------------
-- INSPECT RESULTS
-----------------------------------------------------------

-- Inspect the accuracy metrics of your model. 
CALL my_model!SHOW_EVALUATION_METRICS();

-- Inspect the relative importance of your features, including auto-generated features. 
CALL my_model!EXPLAIN_FEATURE_IMPORTANCE();
