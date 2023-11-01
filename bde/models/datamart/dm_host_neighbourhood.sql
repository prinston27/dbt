WITH base AS (
    SELECT 
        listing_id,
        host_id,
        TO_CHAR(DATE_TRUNC('month', date::date), 'Month YYYY') AS month_year, 
        host_neighbourhood AS host_neighbourhood_lga,
        has_availability,
        host_is_superhost,
        availability_30,
        price
    FROM {{ ref('facts_listing') }}
),

active_listings AS (
    SELECT 
        host_neighbourhood_lga,
        month_year,
        COUNT(DISTINCT listing_id) AS active_listings_count,
        COUNT(DISTINCT CASE WHEN host_is_superhost = 't' THEN host_id END) AS superhost_count
    FROM base
    WHERE has_availability = 't'
    GROUP BY 1,2
),

total_listings AS (
    SELECT 
        host_neighbourhood_lga,
        month_year,
        COUNT(DISTINCT listing_id) AS total_listings_count,
        COUNT(DISTINCT host_id) AS total_hosts_count
    FROM base
    GROUP BY 1,2
),

estimated_revenue AS (
    SELECT 
        host_neighbourhood_lga,
        month_year,
        SUM((30 - availability_30) * price) AS total_estimated_revenue
    FROM base
    WHERE has_availability = 't'
    GROUP BY 1,2
)

SELECT DISTINCT
    a.host_neighbourhood_lga,
    a.month_year,
    COALESCE(b.total_hosts_count, 0) AS distinct_host_count,
    COALESCE(c.total_estimated_revenue, 0) AS total_estimated_revenue,
    COALESCE(c.total_estimated_revenue / NULLIF(b.total_hosts_count, 0), 0) AS estimated_revenue_per_host,
    COALESCE(d.active_listings_count, 0) AS active_listings_count,
    (COALESCE(d.active_listings_count, 0) * 1.0 / NULLIF(b.total_listings_count, 0)) * 100 AS active_listing_rate,
    (COALESCE(d.superhost_count, 0) * 1.0 / NULLIF(b.total_hosts_count, 0)) * 100 AS superhost_rate,
    COALESCE(((c.total_estimated_revenue - LAG(c.total_estimated_revenue, 1) OVER (PARTITION BY a.host_neighbourhood_lga ORDER BY a.month_year)) / NULLIF(LAG(c.total_estimated_revenue, 1) OVER (PARTITION BY a.host_neighbourhood_lga ORDER BY a.month_year), 0)) * 100, 0) AS revenue_percentage_change_mtm
FROM base a
LEFT JOIN total_listings b ON a.host_neighbourhood_lga = b.host_neighbourhood_lga AND a.month_year = b.month_year
LEFT JOIN estimated_revenue c ON a.host_neighbourhood_lga = c.host_neighbourhood_lga AND a.month_year = c.month_year
LEFT JOIN active_listings d ON a.host_neighbourhood_lga = d.host_neighbourhood_lga AND a.month_year = d.month_year
ORDER BY 1,2
