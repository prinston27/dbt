{% snapshot snapshot_host %}
    
    {{
        config(
            target_schema='snapshots',
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
