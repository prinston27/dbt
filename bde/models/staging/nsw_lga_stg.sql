WITH stage AS (
    SELECT
        TRIM(LGA_CODE) AS LGA_CODE,
        TRIM(LGA_NAME) AS LGA_NAME
    FROM RAW.nsw_lga_code
)

SELECT * FROM stage
