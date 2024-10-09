with source as (
    select 
        * 
    from {{ source('escaleos_eventhub', 'eventhub_attribution') }}
)
, build_source as (
    SELECT
        event_id
        ,CASE 
            WHEN LOWER(utm_source) = 'bing' THEN  COALESCE(REPLACE(id_user_ads, '', NULL), SPLIT_PART(SPLIT_PART(SPLIT_PART(page_url, 'msclkid=', 2), '&', 1), '#', 1))
            ELSE id_user_ads 
            END AS id_user_ads
        ,page_url
        ,COALESCE(referer_url, 'no_referer_url') AS referer_url
        ,lead_id
        ,event_type
        ,device_family
        ,COALESCE(session_id, 'no_session_id') AS session_id
        ,COALESCE(page_path, '/') AS page_path
        ,page_host
        ,LOWER(product) AS product
        ,LOWER(vertical) AS vertical
    FROM source
    WHERE event_type IN ('click', 'pageview', 'form_submit')
)
select
    *
from build_source