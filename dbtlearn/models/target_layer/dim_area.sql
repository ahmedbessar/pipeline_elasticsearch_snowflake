{{ config(materialized='incremental') }}

WITH area_data AS (
    SELECT DISTINCT
        AREAS AS AreaName,
        CURRENT_TIMESTAMP() AS RefreshDate -- Adding dynamic refresh date
    FROM WASEET.RAW.PROD_POSTS_KW_ELS
)

SELECT
    ROW_NUMBER() OVER (ORDER BY AreaName) AS AreaID,
    AreaName,
    RefreshDate
FROM area_data

{% if is_incremental() %}
  WHERE RefreshDate > (SELECT MAX(RefreshDate) FROM {{ this }})
  OR RefreshDate IS NULL
{% endif %}
