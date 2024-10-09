with source as (
    select
        *
    from {{ source('homeservices_claro', 'bing_ads_insights') }}
)
, build_source as (
    SELECT 
        CAST(time_period AS DATE) AS reference_date,
        campaign_id,
        null as customer_id,
        null as business_id,
        campaign_name AS campaign,
        'claro' as brand,
        'bing' as source,
        CAST(spend AS DOUBLE) AS cost, 
        CAST(impressions AS INT) AS impressions,
        CAST(clicks AS INT) AS clicks
    FROM hive_metastore.trusted_homeservices_claro.bing_ads_insights
    WHERE LOWER(campaign_name) NOT LIKE '%oi%'
)
select
    *
from build_source