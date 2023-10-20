{{
    config(
        unique_key='brand_id'
    )
}}

with

source  as (

    select * from {{ ref('facts_snapshot') }}

),

base AS (
    SELECT
        DATE_PART('YEAR', SCRAPED_DATE::DATE) AS year,
        DATE_PART('MONTH', SCRAPED_DATE::DATE) AS month,
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES,
        CASE WHEN HAS_AVAILABILITY = 't' THEN 1 ELSE 0 END AS is_active,
        HOST_ID,
        CASE WHEN HOST_IS_SUPERHOST = 't' THEN 1 ELSE 0 END AS is_superhost,
        PRICE,
        REVIEW_SCORES_RATING,
        NUMBER_OF_REVIEWS,
        30 - availability_30 AS Number_of_stays,
        (30 - availability_30) * PRICE AS EstimatedRevenuePerListing
    FROM source
),

agg AS (
    SELECT
        year,
        month,
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES,
        COUNT(*) AS TotalListings,
        SUM(is_active) AS ActiveListingsCount,
        COUNT(DISTINCT HOST_ID) AS DistinctHostCount,
        COUNT(DISTINCT CASE WHEN is_superhost = 1 THEN HOST_ID END) AS DistinctSuperHostCount,
        MIN(CASE WHEN is_active = 1 THEN PRICE END) AS MinPrice,
        MAX(CASE WHEN is_active = 1 THEN PRICE END) AS MaxPrice,
        AVG(CASE WHEN is_active = 1 THEN PRICE END) AS AvgPrice,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN is_active = 1 THEN PRICE END) AS MedianPrice,
        AVG(CASE WHEN is_active = 1 THEN REVIEW_SCORES_RATING END) AS AvgReviewScore,
        SUM(Number_of_stays) FILTER (WHERE is_active = 1) AS TotalNumber_of_stays,
        SUM(EstimatedRevenuePerListing) FILTER (WHERE is_active = 1) AS TotalEstimatedRevenue
    FROM base
    GROUP BY 
        year,
        month,
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES
)

SELECT 
    PROPERTY_TYPE,
    ROOM_TYPE,
    ACCOMMODATES,
    TO_CHAR(TO_DATE(year || '-' || month || '-01', 'YYYY-MM-DD'), 'Month YYYY') AS "Month/Year",
    CASE 
        WHEN TotalListings = 0 THEN NULL
        ELSE ActiveListingsCount::FLOAT / TotalListings * 100
    END AS ActiveListingRate,
    CASE 
        WHEN DistinctHostCount = 0 THEN NULL
        ELSE DistinctSuperHostCount::FLOAT / DistinctHostCount * 100 
    END AS SuperhostRate,
    MinPrice,
    MaxPrice,
    AvgPrice,
    MedianPrice, 
    AvgReviewScore,
    TotalNumber_of_stays,
    TotalEstimatedRevenue,
    CASE 
        WHEN DistinctHostCount = 0 THEN NULL
        ELSE TotalEstimatedRevenue::FLOAT / DistinctHostCount 
    END AS EstimatedRevenuePerHost
FROM agg
ORDER BY PROPERTY_TYPE, ROOM_TYPE, ACCOMMODATES, year, month
