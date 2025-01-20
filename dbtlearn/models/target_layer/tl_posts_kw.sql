{{
  config(
    materialized = 'incremental'
    )
}}
WITH tl_posts AS (
  SELECT * FROM WASEET.RAW.PROD_POSTS_KW_ELS
)
SELECT  *
FROM tl_posts
{% if is_incremental() %}
  WHERE CREATEDAT > (SELECT MAX(CREATEDAT) FROM {{ this }})
     OR CREATEDAT IS NULL
{% endif %}