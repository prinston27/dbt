{% snapshot snapshot_property %}
    
    {{
        config(
            target_schema='snapshots',
            strategy='timestamp',
            updated_at='SCRAPED_DATE',
            unique_key='LISTING_ID'
        )
    }}
    
    SELECT
        LISTING_ID,
        PROPERTY_TYPE,
        SCRAPED_DATE
    FROM RAW.facts

{% endsnapshot %}
