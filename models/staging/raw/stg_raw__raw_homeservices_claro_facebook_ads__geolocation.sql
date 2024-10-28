with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_facebook_ads__geolocation') }}

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
        cpc,
        cpm,
        date,
        date_start,
        date_stop,
        impressions,
        lake_created_at,
        region,
        spend,
        business_id,
        year,
        month,
        day

    from source

)

select * from renamed
