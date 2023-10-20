{{
    config(
        unique_key='brand_id'
    )
}}

with

source  as (

    select * from {{ ref('facts_snapshot') }}

),

transformed_host_neighbourhood AS (
    SELECT
        LISTING_NEIGHBOURHOOD AS host_neighbourhood_lga,
        HOST_ID,
        TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD') AS scraped_date,
        CASE WHEN HAS_AVAILABILITY = 't' THEN 1 ELSE 0 END AS IsActive,
        CASE WHEN HOST_IS_SUPERHOST = 't' THEN 1 ELSE 0 END AS IsSuperhost,
        (30 - AVAILABILITY_30) * PRICE AS EstimatedRevenuePerListing
    FROM source
),

aggregated AS (
    SELECT
        t.host_neighbourhood_lga,
        EXTRACT(MONTH FROM t.scraped_date) AS month,
        EXTRACT(YEAR FROM t.scraped_date) AS year,
        COUNT(DISTINCT t.HOST_ID) AS distinct_host_count,
        ROUND(SUM(EstimatedRevenuePerListing)::numeric,4) AS EstimatedRevenue,
        ROUND(SUM(EstimatedRevenuePerListing)::numeric / NULLIF(COUNT(DISTINCT t.HOST_ID), 0)::numeric,4) AS RevenuePerHost
    FROM transformed_host_neighbourhood t
    GROUP BY t.host_neighbourhood_lga, EXTRACT(MONTH FROM t.scraped_date), EXTRACT(YEAR FROM t.scraped_date)
)

SELECT * FROM aggregated
ORDER BY host_neighbourhood_lga, year, month
