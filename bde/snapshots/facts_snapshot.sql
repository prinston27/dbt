{% snapshot facts_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='check',
          unique_key='LISTING_ID',
          check_cols=['LISTING_ID','SCRAPE_ID','SCRAPED_DATE','HOST_ID','HOST_NAME','HOST_SINCE','HOST_IS_SUPERHOST','HOST_NEIGHBOURHOOD','LISTING_NEIGHBOURHOOD',
                     'PROPERTY_TYPE','ROOM_TYPE','ACCOMMODATES','PRICE','HAS_AVAILABILITY','AVAILABILITY_30','NUMBER_OF_REVIEWS','REVIEW_SCORES_RATING',
                     'REVIEW_SCORES_ACCURACY','REVIEW_SCORES_CLEANLINESS','REVIEW_SCORES_CHECKIN','REVIEW_SCORES_COMMUNICATION','REVIEW_SCORES_VALUE'],
        )
    }}

select * from {{source('raw','facts') }}

{% endsnapshot %}