{{ config(
    materialized='view',
    unique_key='listing_id'
) }}

WITH source AS (
    SELECT
        listing_id,
        TO_DATE(date, 'YYYY-MM-DD') AS scraped_date,
        host_id,
        CASE WHEN host_is_superhost = 't' THEN 1 ELSE 0 END AS IsSuperhost,
        listing_neighbourhood,
        price,
        CASE WHEN has_availability = 't' THEN 1 ELSE 0 END AS IsActive,
        review_scores_rating,
        (30 - availability_30) AS number_of_stays,
        (30 - availability_30) * price AS EstimatedRevenuePerListing
    FROM warehouse.facts_listing
),

monthly_aggregates AS (
    SELECT
        listing_neighbourhood,
        EXTRACT(MONTH FROM scraped_date) AS month,
        EXTRACT(YEAR FROM scraped_date) AS year,
        COUNT(*) AS total_listings,
        COUNT(DISTINCT CASE WHEN IsActive = 1 THEN listing_id END) AS active_listings,
        COUNT(DISTINCT host_id) AS distinct_host_count,
        COUNT(DISTINCT CASE WHEN IsSuperhost = 1 THEN host_id END) AS distinct_superhost_count,
        AVG(CASE WHEN IsActive = 1 THEN price END) AS avg_price_active,
        MIN(CASE WHEN IsActive = 1 THEN price END) AS min_price_active,
        MAX(CASE WHEN IsActive = 1 THEN price END) AS max_price_active,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY CASE WHEN IsActive = 1 THEN price END) AS median_price_active,
        AVG(CASE WHEN IsActive = 1 THEN review_scores_rating END) AS avg_review_score,
        SUM(CASE WHEN IsActive = 1 THEN EstimatedRevenuePerListing END) AS total_revenue,
        SUM(CASE WHEN IsActive = 1 THEN number_of_stays END) AS total_stays
    FROM source
    GROUP BY listing_neighbourhood, EXTRACT(MONTH FROM scraped_date), EXTRACT(YEAR FROM scraped_date)
)

SELECT 
    ma.listing_neighbourhood,
    ma.month,
    ma.year,
    (ma.active_listings::float / NULLIF(ma.total_listings, 0)) * 100 AS active_listings_rate,
    ma.min_price_active,
    ma.max_price_active,
    ma.median_price_active,
    ma.avg_price_active,
    ma.distinct_host_count,
    (ma.distinct_superhost_count::float / NULLIF(ma.distinct_host_count, 0)) * 100 AS superhost_rate,
    ma.avg_review_score,
    LAG(ma.active_listings, 1) OVER (PARTITION BY ma.listing_neighbourhood ORDER BY ma.year, ma.month) AS previous_month_active,
    LAG(ma.total_listings - ma.active_listings, 1) OVER (PARTITION BY ma.listing_neighbourhood ORDER BY ma.year, ma.month) AS previous_month_inactive,
    ((ma.active_listings - COALESCE(LAG(ma.active_listings, 1) OVER (PARTITION BY ma.listing_neighbourhood ORDER BY ma.year, ma.month), ma.active_listings))::float / NULLIF(COALESCE(LAG(ma.active_listings, 1) OVER (PARTITION BY ma.listing_neighbourhood ORDER BY ma.year, ma.month), 1), 0)) * 100 AS pct_change_active,
    ((ma.total_listings - ma.active_listings - COALESCE(LAG(ma.total_listings - ma.active_listings, 1) OVER (PARTITION BY ma.listing_neighbourhood ORDER BY ma.year, ma.month), (ma.total_listings - ma.active_listings)))::float / NULLIF(COALESCE(LAG(ma.total_listings - ma.active_listings, 1) OVER (PARTITION BY ma.listing_neighbourhood ORDER BY ma.year, ma.month), 1), 0)) * 100 AS pct_change_inactive,
    ma.total_stays,
    ma.total_revenue / NULLIF(ma.active_listings, 0) AS avg_revenue_per_active_listing
FROM monthly_aggregates ma
ORDER BY ma.listing_neighbourhood, ma.year, ma.month
