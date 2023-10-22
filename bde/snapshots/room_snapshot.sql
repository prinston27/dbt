{% snapshot snapshot_room %}
    
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
        ROOM_TYPE,
        ACCOMMODATES,
        SCRAPED_DATE
    FROM RAW.facts

{% endsnapshot %}
