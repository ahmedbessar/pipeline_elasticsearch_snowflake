{{ config(materialized='incremental') }}

WITH city_data AS (
    SELECT DISTINCT
        CITIES AS CityName,
        AREAS AS AreaName,
        CURRENT_TIMESTAMP() AS RefreshDate -- Adding dynamic refresh date
    FROM WASEET.RAW.PROD_POSTS_KW_ELS
)

SELECT
    ROW_NUMBER() OVER (ORDER BY CityName) AS CityID,
    CityName,
    ar.AreaID,
    RefreshDate
FROM city_data
LEFT JOIN {{ ref('dim_area') }} ar
    ON city_data.AreaName = ar.AreaName

{% if is_incremental() %}
  WHERE RefreshDate > (SELECT MAX(RefreshDate) FROM {{ this }})
  OR RefreshDate IS NULL
{% endif %}
