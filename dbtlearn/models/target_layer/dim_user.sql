{{ config(materialized='incremental') }}

WITH user_data AS (
    SELECT DISTINCT
        USERNAME AS Username,
        PHONENUMBER AS PhoneNumber,
        CURRENT_TIMESTAMP() AS RefreshDate -- Adding dynamic refresh date
    FROM WASEET.RAW.PROD_POSTS_KW_ELS
)

SELECT
    ROW_NUMBER() OVER (ORDER BY Username) AS UserID,
    Username,
    PhoneNumber,
    RefreshDate
FROM user_data

{% if is_incremental() %}
  WHERE RefreshDate > (SELECT MAX(RefreshDate) FROM {{ this }})
  OR RefreshDate IS NULL
{% endif %}
