-- Merging the two nsw_lga staging tables
WITH merged_nsw_lga AS (
    SELECT
        c.LGA_CODE,
        c.LGA_NAME,
        s.SUBURB_NAME
    FROM staging.nsw_lga_stg c
    LEFT JOIN staging.nsw_lga_suburb_stg s ON c.LGA_NAME = s.LGA_NAME
),

-- Getting facts data
stage_facts AS (
    SELECT
        LISTING_ID,
        SCRAPED_DATE as "date",
        HOST_ID,
        HOST_NAME,
        HOST_SINCE,
        HOST_IS_SUPERHOST,
        HOST_NEIGHBOURHOOD,
        LISTING_NEIGHBOURHOOD,
        PROPERTY_TYPE,
        ROOM_TYPE,
        ACCOMMODATES,
        PRICE,
        HAS_AVAILABILITY,
        AVAILABILITY_30,
        NUMBER_OF_REVIEWS,
        REVIEW_SCORES_RATING
    FROM staging.facts_stg
)

-- Merging facts data with the merged nsw_lga data
SELECT 
    f.*,
    m.LGA_CODE as host_neighbourhood_code,
    m.suburb_name as host_neighbourhood_name
FROM stage_facts f
LEFT JOIN merged_nsw_lga m ON f.LISTING_NEIGHBOURHOOD = m.LGA_NAME

