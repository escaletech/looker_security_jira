with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_google_ads__geographic_view') }}

),

renamed as (

    select
        ad_group_id,
        campaign_id,
        customer_id,
        lake_created_at,
        segments_date,
        segments_geo_target_city,
        segments_geo_target_state,
        business_id,
        month,
        day

    from source

)

select * from renamed
