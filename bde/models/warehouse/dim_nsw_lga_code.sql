{{ 
    config(
        unique_key='lga_code'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('nsw_lga_stg') }}
),

renamed AS (
    SELECT
        lga_code::text AS lga_code,
        lga_name::text AS lga_name
    FROM source
),

unknown AS (
    SELECT
        '0'::text AS lga_code,
        'unknown'::text AS lga_name
)

SELECT * FROM unknown
UNION ALL
SELECT * FROM renamed
