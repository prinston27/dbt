{% snapshot host_snapshot %}
    
    {{
        config(
            target_schema='raw',
            strategy='timestamp',
            updated_at='SCRAPED_DATE',
            unique_key='HOST_ID'
        )
    }}
    
    SELECT
        HOST_ID,
        HOST_NAME,
        HOST_SINCE,
        HOST_IS_SUPERHOST,
        HOST_NEIGHBOURHOOD,
        SCRAPED_DATE
    FROM RAW.facts

{% endsnapshot %}
