{{ 
    config(
        unique_key='host_id'
    )
}}

WITH source AS (
    SELECT * FROM {{ ref('host_snapshot') }}
),

renamed AS (
    SELECT
        host_id::int8 AS host_id,
        host_name::varchar(100) AS host_name,
        host_since::varchar(15) AS host_since,
        host_is_superhost::varchar(5) AS host_is_superhost,
        host_neighbourhood::varchar(100) AS host_neighbourhood,
        scraped_date::varchar(15) AS scraped_date,
        dbt_scd_id::text AS dbt_scd_id,
        CASE 
            WHEN dbt_valid_from::varchar(15) = (SELECT MIN(dbt_valid_from)::varchar(15) FROM source) THEN '1900-01-01' 
            ELSE dbt_valid_from::varchar(15)
        END AS dbt_valid_from,
        dbt_valid_to::text AS dbt_valid_to
    FROM source
),

unknown AS (
    SELECT
        0::int8 AS host_id,
        'unknown'::varchar(100) AS host_name,
        NULL::varchar(15) AS host_since,
        NULL::varchar(5) AS host_is_superhost,
        'unknown'::varchar(100) AS host_neighbourhood,
        NULL::varchar(15) AS scraped_date,
        'unknown'::text AS dbt_scd_id,
        '1900-01-01'::varchar(15) AS dbt_valid_from,
        NULL::text AS dbt_valid_to
)

SELECT * FROM unknown
UNION ALL
SELECT * FROM renamed
