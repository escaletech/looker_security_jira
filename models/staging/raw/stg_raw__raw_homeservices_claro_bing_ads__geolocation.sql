with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_bing_ads__geolocation') }}

),

renamed as (

    select
        account_name,
        time_period,
        campaign_id,
        account_id,
        city,
        state,
        conversions,
        lake_created_at,
        month,
        day

    from source

)

select * from renamed