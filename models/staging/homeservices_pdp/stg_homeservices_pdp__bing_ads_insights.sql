with source as (
    select
        *
    from {{ source('source_name', 'object_name') }}
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
        SUM(CAST(spend AS DOUBLE)) AS cost, 
        SUM(CAST(impressions AS INT)) AS impressions,
        SUM(CAST(clicks AS INT)) AS clicks
    FROM hive_metastore.trusted_homeservices_pdp.bing_ads_insights
    GROUP BY all
)
select
    *
from source