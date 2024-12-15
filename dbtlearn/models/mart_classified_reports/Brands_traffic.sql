WITH DirectChildren AS (
    SELECT 
        ID AS DirectChildID,
        ParentID,
        CategoryNameEn AS CategoryName,
        ParentNameEn AS ParentName
    FROM 
        WASEET.RAW.category_hierarchy
    WHERE 
        ParentID = 29
),
GrandChildren AS (
    SELECT 
        ch.ID AS CategoryID,
        ch.ParentID,
        ch.CategoryNameEn AS CategoryName,
        ch.ParentNameEn AS ParentName,
        dc.DirectChildID,
        dc.CategoryName AS DirectChildIDName,
        dc.ParentName AS ParentName_dc
    FROM 
        WASEET.RAW.category_hierarchy ch
    INNER JOIN 
        DirectChildren dc ON ch.ParentID = dc.DirectChildID
),
BaseQuery AS (
    SELECT 
        c.id AS PostID,
        c.created,
        c.city,
        c.created_by,
        c.username,
        c.leaf_taxonomy_id,
        c.phone_number,
        c.taxonomy_status,
        c.sticky_payment_start_time,
        c.sticky_expiry,
        COALESCE(gc.DirectChildID, dc.DirectChildID) AS DirectChildID,
        COALESCE(gc.DirectChildIDName, dc.CategoryName) AS DirectChildName,
        COALESCE(gc.ParentName_dc, dc.ParentName) AS ParentName,

        CASE 
            WHEN c.taxonomy_status = 0 THEN 1
            ELSE 0
        END AS is_paid,

        CASE 
            WHEN c.sticky_payment_start_time IS NOT NULL 
                 AND c.sticky_expiry IS NOT NULL THEN 1
            ELSE 0
        END AS is_sticky,

        CASE 
            WHEN c.taxonomy_status <> 0 THEN 1
            ELSE 0
        END AS is_free
    FROM 
        WASEET.RAW.classifieds c
    LEFT JOIN 
        DirectChildren dc 
        ON c.leaf_taxonomy_id = dc.DirectChildID
    LEFT JOIN 
        GrandChildren gc
        ON c.leaf_taxonomy_id = gc.CategoryID
    WHERE 
        c.data_type = 'global'
)
SELECT 
    bq.DirectChildID AS CategoryID,
    bq.DirectChildName AS CategoryName,
    bq.ParentName AS ParentName,

    SUM(COALESCE(bq.is_paid, 0) + COALESCE(bq.is_free, 0)) AS FreePaidTotalCount,

    SUM(
        CASE 
            WHEN bq.created_by = 'user' THEN COALESCE(bq.is_paid, 0) + COALESCE(bq.is_free, 0)
            ELSE 0
        END
    ) AS FreePaidOrganicCount,

    COUNT(DISTINCT bq.username) AS TotalFreePaidUsers,

    SUM(COALESCE(bq.is_sticky, 0)) AS StickyTotalCount,

    SUM(
        CASE 
            WHEN bq.created_by = 'user' THEN COALESCE(bq.is_sticky, 0)
            ELSE 0
        END
    ) AS StickyOrganicCount,

    COUNT(DISTINCT CASE WHEN bq.is_sticky = 1 THEN bq.username END) AS TotalStickyUsers

FROM 
    BaseQuery bq
GROUP BY 
    bq.DirectChildID, bq.DirectChildName, bq.ParentName
HAVING 
    bq.DirectChildID IS NOT NULL
ORDER BY 
    bq.DirectChildID