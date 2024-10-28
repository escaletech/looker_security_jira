with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_google_ads__ad_group') }}

),

renamed as (

    select
        ad_group_id,
        ad_group_name,
        ad_group_tracking_url_template,
        campaign_advertising_channel_type,
        campaign_id,
        campaign_name,
        customer_descriptive_name,
        customer_id,
        lake_created_at,
        metrics_clicks,
        metrics_conversions,
        metrics_cost_micros,
        metrics_impressions,
        metrics_search_absolute_top_impression_share,
        metrics_search_budget_lost_top_impression_share,
        metrics_search_impression_share,
        metrics_search_rank_lost_impression_share,
        metrics_search_rank_lost_top_impression_share,
        metrics_video_views,
        segments_date,
        segments_device,
        business_id,
        month,
        day

    from source

)

select * from renamed
