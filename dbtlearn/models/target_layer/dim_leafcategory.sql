{{ config(materialized='incremental') }}

WITH subcategory_data AS (
    SELECT DISTINCT
        EXTRA_SUBCATEGORY AS leafCategory,
        EXTRA_SUBCATEGORY_SLUG AS leafCategorySlug,
        MAINCATEGORY_SLUG AS CategorySlug,
        CURRENT_TIMESTAMP() AS RefreshDate -- Dynamic refresh date
    FROM WASEET.RAW.PROD_POSTS_KW_ELS
)

SELECT
    ROW_NUMBER() OVER (ORDER BY SubCategorySlug) AS SubCategoryID,
    SubCategory,
    SubCategorySlug,
    cat.CategoryID,
    subcategory_data.RefreshDate
FROM subcategory_data
LEFT JOIN {{ ref('dim_category') }} cat
    ON subcategory_data.CategorySlug = cat.CategorySlug

{% if is_incremental() %}
WHERE subcategory_data.RefreshDate > (SELECT MAX(RefreshDate) FROM {{ this }})
{% endif %}
