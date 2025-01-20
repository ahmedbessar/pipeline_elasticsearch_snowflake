{{ config(materialized='incremental') }}

WITH rejectreason_data AS (
    SELECT DISTINCT
        REJECTEDREASONID AS RejectReasonID,
        CASE
            WHEN REJECTEDREASONID = 1 THEN 'Invalid Content'
            WHEN REJECTEDREASONID = 2 THEN 'Policy Violation'
            ELSE 'Other'
        END AS RejectReasonName,
        CURRENT_TIMESTAMP() AS RefreshDate -- Adding dynamic refresh date
    FROM WASEET.RAW.PROD_POSTS_KW_ELS
    WHERE REJECTEDREASONID IS NOT NULL
)

SELECT
    RejectReasonID,
    RejectReasonName,
    RefreshDate
FROM rejectreason_data

{% if is_incremental() %}
WHERE RejectReasonID NOT IN (SELECT RejectReasonID FROM {{ this }})
{% endif %}
