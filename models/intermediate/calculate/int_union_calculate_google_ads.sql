SELECT
    reference_date,
    campaign_id,
    customer_id,
    business_id,
    campaign,
    brand,
    source,
    SUM(cost) AS cost, 
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks
FROM {{ ref('stg_homeservices_claro__google_ads_ad_group') }}
GROUP BY all
union all
SELECT
    reference_date,
    campaign_id,
    customer_id,
    business_id,
    campaign,
    brand,
    source,
    SUM(cost) AS cost, 
    SUM(impressions) AS impressions,
    SUM(clicks) AS clicks
FROM {{ ref('stg_homeservices_pdp__google_ads_ad_group') }}
GROUP BY all