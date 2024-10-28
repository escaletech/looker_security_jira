with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_facebook_ads__insights') }}

),

renamed as (

    select
        account_id,
        account_name,
        ad_id,
        ad_name,
        adset_name,
        campaign_id,
        campaign_name,
        clicks,
        cost_per_inline_link_click,
        cost_per_unique_click,
        cpc,
        cpm,
        ctr,
        date,
        date_start,
        date_stop,
        frequency,
        impressions,
        inline_link_clicks,
        lake_created_at,
        quality_ranking,
        reach,
        spend,
        unique_clicks,
        unique_ctr,
        unique_inline_link_clicks,
        business_id,
        year,
        month,
        day

    from source

)

select * from renamed
