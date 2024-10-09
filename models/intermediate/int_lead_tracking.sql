SELECT
    event_id
    ,id_user_ads
    ,lead_id
    ,session_id
    ,page_url
    ,referer_url
    ,event_type
    ,device_family
    ,page_path
    ,page_host
    ,product
    ,vertical
FROM {{ ref('stg_escaleos_eventhub__eventhub_attribution') }}