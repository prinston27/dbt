{{
    config(
        unique_key='lga_code_2016'
    )
}}

select * from {{ ref('g01_stg') }}