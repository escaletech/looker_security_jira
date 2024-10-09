with source as (
    select
        *
    from {{ source('homeservices_pdp', 'google_ads_ad_group') }}
)
, build_source as (
    SELECT
        CAST(segments_date AS DATE) AS reference_date,
        campaign_id,
        customer_id,
        business_id,
        campaign_name AS campaign,
        'pdp' AS brand,
        'google' AS source,
        CAST(metrics_cost_micros AS DOUBLE) / 1000000 AS cost, 
        CAST(metrics_impressions AS INT) AS impressions,
        CAST(metrics_clicks AS INT) AS clicks
    FROM trusted_homeservices_pdp.google_ads_ad_group
)
select
    *
from build_source