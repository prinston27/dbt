{% snapshot property_snapshot %}
    
    {{
        config(
            target_schema='raw',
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
