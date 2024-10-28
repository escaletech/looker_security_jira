with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_google_ads__campaign') }}

),

renamed as (

    select
        campaign_advertising_channel_type,
        campaign_campaign_budget,
        campaign_id,
        campaign_name,
        campaign_status,
        customer_id,
        lake_created_at,
        metrics_conversions,
        metrics_cost_micros,
        metrics_search_click_share,
        metrics_search_impression_share,
        metrics_video_views,
        segments_date,
        segments_device,
        business_id,
        month,
        day

    from source

)

select * from renamed
