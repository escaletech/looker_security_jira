with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_bing_ads__insights') }}

),

renamed as (

    select
        account_name,
        campaign_id,
        campaign_name,
        spend,
        impressions,
        clicks,
        conversions,
        average_position,
        time_period,
        account_id,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
