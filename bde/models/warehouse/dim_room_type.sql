{{ 
    config(
        unique_key='listing_id'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('room_snapshot') }}
),

renamed AS (
    SELECT
        listing_id::varchar(100),  
        room_type::varchar(100),
        accommodates::int8,
        scraped_date::varchar(15),
        dbt_scd_id::text,
        CASE 
            WHEN dbt_valid_from::varchar(15) = (SELECT MIN(dbt_valid_from)::varchar(15) FROM source) THEN '1900-01-01' 
            ELSE dbt_valid_from::varchar(15)
        END AS dbt_valid_from,
        dbt_valid_to::text
    FROM source
),

unknown AS (
    SELECT
        '0'::varchar(100) AS listing_id,  
        'unknown'::varchar(100) AS room_type,
        NULL::int8 AS accommodates,
        NULL::varchar(15) AS scraped_date,
        'unknown'::text AS dbt_scd_id,
        '1900-01-01'::varchar(15) AS dbt_valid_from,
        NULL::text AS dbt_valid_to
)

SELECT * FROM unknown
UNION ALL
SELECT * FROM renamed
