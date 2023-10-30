WITH stage AS (
    SELECT
        TRIM(LGA_NAME) AS LGA_NAME,
        TRIM(SUBURB_NAME) AS SUBURB_NAME
    FROM RAW.nsw_lga_suburb
)

SELECT * FROM stage
