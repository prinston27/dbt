WITH base AS (
    SELECT 
        LISTING_NEIGHBOURHOOD,
        EXTRACT(MONTH FROM TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD')) AS MONTH,
        EXTRACT(YEAR FROM TO_DATE(SCRAPED_DATE, 'YYYY-MM-DD')) AS YEAR,
        CASE WHEN HAS_AVAILABILITY = 't' THEN 1 ELSE 0 END AS IsActive,
        CASE WHEN HOST_IS_SUPERHOST = 't' THEN 1 ELSE 0 END AS IsSuperhost,
        REVIEW_SCORES_RATING,
        HOST_ID,
        PRICE,
        NUMBER_OF_REVIEWS,
        PRICE * AVAILABILITY_30 AS EstimatedRevenuePerListing
    FROM RAW.facts
),

metrics AS (
    SELECT
        b.LISTING_NEIGHBOURHOOD,
        b.MONTH,
        b.YEAR,
        SUM(b.IsActive) AS ActiveListingsCount,
        COUNT(*) AS TotalListings,
        SUM(b.IsSuperhost) AS SuperhostCount,
        COUNT(DISTINCT b.HOST_ID) AS DistinctHostCount,
        AVG(CASE WHEN b.IsActive = 1 THEN b.REVIEW_SCORES_RATING ELSE NULL END) AS AvgReviewScore,
        SUM(b.NUMBER_OF_REVIEWS) AS TotalStays,
        AVG(b.EstimatedRevenuePerListing) AS AvgEstimatedRevenue,
        MIN(CASE WHEN b.IsActive = 1 THEN b.PRICE ELSE NULL END) AS MinPrice,
        MAX(CASE WHEN b.IsActive = 1 THEN b.PRICE ELSE NULL END) AS MaxPrice,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY CASE WHEN b.IsActive = 1 THEN b.PRICE ELSE NULL END) AS MedianPrice,
        AVG(CASE WHEN b.IsActive = 1 THEN b.PRICE ELSE NULL END) AS AvgPrice,
        LAG(SUM(b.IsActive)) OVER (PARTITION BY b.LISTING_NEIGHBOURHOOD ORDER BY b.YEAR, b.MONTH) AS PrevActiveListingsCount,
        LAG(COUNT(*)) OVER (PARTITION BY b.LISTING_NEIGHBOURHOOD ORDER BY b.YEAR, b.MONTH) - LAG(SUM(b.IsActive)) OVER (PARTITION BY b.LISTING_NEIGHBOURHOOD ORDER BY b.YEAR, b.MONTH) AS PrevInactiveListingsCount
    FROM base b
    GROUP BY b.LISTING_NEIGHBOURHOOD, b.MONTH, b.YEAR
)

SELECT
    m.LISTING_NEIGHBOURHOOD,
    TO_CHAR(TO_DATE(m.YEAR || '-' || m.MONTH || '-01', 'YYYY-MM-DD'), 'Month YYYY') AS "Month/Year",
    CASE WHEN m.TotalListings != 0 THEN ROUND((m.ActiveListingsCount::numeric / m.TotalListings::numeric), 4) ELSE NULL END AS ActiveRate,
    CASE WHEN m.ActiveListingsCount != 0 THEN ROUND((m.SuperhostCount::numeric / m.ActiveListingsCount::numeric), 4) ELSE NULL END AS SuperhostRate,
    m.DistinctHostCount,
    ROUND(m.AvgReviewScore::numeric, 4) AS AvgReviewScore,
    m.TotalStays,
    ROUND(m.AvgEstimatedRevenue::numeric, 4) AS AvgEstimatedRevenue,
    m.MinPrice,
    m.MaxPrice,
    m.MedianPrice,
    ROUND(m.AvgPrice::numeric, 4) AS AvgPrice,
    CASE 
        WHEN m.PrevActiveListingsCount IS NOT NULL AND m.PrevActiveListingsCount != 0 THEN 
            ROUND(((m.ActiveListingsCount - m.PrevActiveListingsCount)::numeric / m.PrevActiveListingsCount::numeric) * 100, 2) 
        ELSE NULL 
    END AS ActiveListingsChangePercent,
    CASE 
        WHEN m.PrevInactiveListingsCount IS NOT NULL AND m.PrevInactiveListingsCount != 0 THEN 
            ROUND(((m.TotalListings - m.ActiveListingsCount - m.PrevInactiveListingsCount)::numeric / m.PrevInactiveListingsCount::numeric) * 100, 2) 
        ELSE NULL 
    END AS InactiveListingsChangePercent
FROM metrics m
ORDER BY m.LISTING_NEIGHBOURHOOD, m.YEAR, m.MONTH
