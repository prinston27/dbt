{% snapshot nsw_lga_suburb_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='check',
          unique_key='LGA_NAME',
          check_cols=['LGA_NAME','SUBURB_NAME'],
        )
    }}

select * from {{source('raw','nsw_lga_suburb') }}

{% endsnapshot %}