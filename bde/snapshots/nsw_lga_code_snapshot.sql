{% snapshot nsw_lga_code_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='check',
          unique_key='LGA_CODE',
          check_cols=['LGA_CODE','LGA_NAME'],
        )
    }}

select * from {{source('raw','nsw_lga_code') }}

{% endsnapshot %}