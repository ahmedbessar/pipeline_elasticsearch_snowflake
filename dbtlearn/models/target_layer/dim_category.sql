{{
  config(
    materialized = 'incremental'
  )
}}

WITH category_data AS (
    SELECT DISTINCT
        MainCategory AS CategoryName,
        MainCategory_Slug AS CategorySlug,
        CURRENT_TIMESTAMP() AS RefreshDate  -- Dynamically create a refresh date
    FROM WASEET.RAW.PROD_POSTS_KW_ELS
)

SELECT
    ROW_NUMBER() OVER (ORDER BY CategorySlug) AS CategoryID,
    CategoryName,
    CategorySlug,
    RefreshDate
FROM category_data

{% if is_incremental() %}
  WHERE RefreshDate > (SELECT MAX(RefreshDate) FROM {{ this }})
     OR RefreshDate IS NULL
{% endif %}
