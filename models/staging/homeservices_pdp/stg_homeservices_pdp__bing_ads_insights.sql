with source as (
    select
        *
    from {{ source('homeservices_pdp', 'bing_ads_insights') }}
)
, build_source as (
    SELECT 
        CAST(time_period AS DATE) AS reference_date,
        campaign_id,
        null as customer_id,
        null as business_id,
        campaign_name AS campaign,
        'pdp' as brand,
        'bing' as source,
        CAST(spend AS DOUBLE) AS cost, 
        CAST(impressions AS INT) AS impressions,
        CAST(clicks AS INT) AS clicks
    FROM hive_metastore.trusted_homeservices_pdp.bing_ads_insights
)
select
    *
from build_source