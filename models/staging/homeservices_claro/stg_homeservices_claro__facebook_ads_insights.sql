with source as (
    select
        *
    from {{ source('homeservices_claro', 'facebook_ads_insights') }}
)
, build_source as (
    SELECT 
        CAST(date_start AS DATE) AS reference_date,
        campaign_id,
        null as customer_id,
        business_id,
        campaign_name AS campaign,
        'claro' as brand,
        'facebook' as source,
        CAST(spend AS DOUBLE) AS cost, 
        CAST(impressions AS INT) AS impressions,
        CAST(clicks AS INT) AS clicks
    FROM source
)
select
    *
from build_source