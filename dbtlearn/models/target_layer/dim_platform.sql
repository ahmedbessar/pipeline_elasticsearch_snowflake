{{ config(materialized='incremental', unique_key='PlatformID') }}

WITH platform_data AS (
    SELECT DISTINCT
        PLATFORM AS PlatformName
    FROM WASEET.RAW.PROD_POSTS_KW_ELS
    {% if is_incremental() %}
        WHERE last_updated >= (SELECT max(RefreshDate) FROM {{ this }})
    {% endif %}
)

SELECT
    ROW_NUMBER() OVER (ORDER BY PlatformName) AS PlatformID,
    PlatformName,
    CURRENT_TIMESTAMP AS RefreshDate
FROM platform_data
