{{ 
    config(
        unique_key='suburb_name'
    )
}}

with

source as (
    select * from {{ ref('nsw_lga_suburb_stg') }}
),

renamed as (
    select
        lga_name,
        suburb_name
    from source
),

unknown as (
    select
        'unknown' as lga_name,
        'unknown' as suburb_name
)

select * from unknown
union all
select * from renamed
