-- File path: models/insights_listing_analysis.sql
{{ config(
  materialized = 'table',
) }}

with raw_data as (
    select
        ENTITYID,
        TITLEENGLISH,
        AREAS,
        CITIES,
        PRICE,
        ISFREE,
        ISNEGOTIABLE,
        CREATEDAT,
        UPDATEDAT,
        REPOSTEDAT,
        REPOSTCOUNT,
        STICKYFROM,
        STICKYTO,
        EXTRA_SUBCATEGORY,
        HANDLEDBY,
        ISSTICKY,
        MEDIACOUNT,
        PLATFORM,
        APPROVEDAT,
        DELETEDAT,
        PUBLISHEDAT,
        ISAPPROVED,
        ISENABLEDCALL,
        ISENABLEDCHAT,
        REJECTEDAT,
        REJECTEDREASONID
    from {{ ref('tl_posts_kw') }} -- Source table reference
),

-- 1. Extract insights on category performance
category_performance as (
    select
        EXTRA_SUBCATEGORY,
        count(ENTITYID) as total_listings,
        count(case when ISSTICKY = true then ENTITYID end) as sticky_listings,
        count(case when ISFREE = true then ENTITYID end) as free_listings,
        avg(PRICE) as avg_price,
        min(PRICE) as min_price,
        max(PRICE) as max_price
    from raw_data
    group by EXTRA_SUBCATEGORY
),

-- 2. Analyze user activity
user_activity as (
    select
        HANDLEDBY,
        count(ENTITYID) as total_posts,
        sum(REPOSTCOUNT) as total_reposts,
        avg(MEDIACOUNT) as avg_media_per_post,
        count(case when ISAPPROVED = true then ENTITYID end) as approved_posts
    from raw_data
    group by HANDLEDBY
),

-- 3. Price trends analysis by category and city
price_trends as (
    select
        EXTRA_SUBCATEGORY,
        CITIES,
        date_trunc('month', PUBLISHEDAT) as publish_month,
        avg(PRICE) as avg_price,
        min(PRICE) as min_price,
        max(PRICE) as max_price
    from raw_data
    where ISFREE = false and PRICE is not null
    group by EXTRA_SUBCATEGORY, CITIES, publish_month
),

-- 4. Sticky listing analysis
sticky_analysis as (
    select
        ENTITYID,
        TITLEENGLISH,
        EXTRA_SUBCATEGORY,
        STICKYFROM,
        STICKYTO,
        datediff('day', STICKYFROM, STICKYTO) as sticky_duration_days,
        PLATFORM
    from raw_data
    where ISSTICKY = true
),

-- 5. Free vs Paid listing comparison
free_vs_paid as (
    select
        case 
            when ISFREE = true then 'Free'
            else 'Paid'
        end as listing_type,
        count(ENTITYID) as total_listings,
        avg(MEDIACOUNT) as avg_media_count,
        avg(REPOSTCOUNT) as avg_reposts,
        count(case when ISNEGOTIABLE = true then ENTITYID end) as negotiable_listings
    from raw_data
    group by listing_type
)

-- Combine all insights into a single output
select
    cp.EXTRA_SUBCATEGORY,
    cp.total_listings,
    cp.sticky_listings,
    cp.free_listings,
    cp.avg_price as avg_category_price,
    ua.HANDLEDBY as top_user,
    ua.total_posts as top_user_posts,
    ua.total_reposts as top_user_reposts,
    pt.publish_month,
    pt.avg_price as monthly_avg_price,
    pt.min_price as monthly_min_price,
    pt.max_price as monthly_max_price,
    sa.sticky_duration_days,
    fvp.listing_type,
    fvp.total_listings as free_paid_total,
    fvp.avg_media_count,
    fvp.avg_reposts
from category_performance cp
left join user_activity ua on cp.EXTRA_SUBCATEGORY = ua.HANDLEDBY
left join price_trends pt on cp.EXTRA_SUBCATEGORY = pt.EXTRA_SUBCATEGORY
left join sticky_analysis sa on cp.EXTRA_SUBCATEGORY = sa.EXTRA_SUBCATEGORY
left join free_vs_paid fvp on cp.EXTRA_SUBCATEGORY = fvp.listing_type
