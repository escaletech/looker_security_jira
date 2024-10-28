with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_google_ads__campaign') }}

),

renamed as (

    select
        campaign_id,
        campaign_name,
        customer_id,
        lake_created_at,
        metrics_search_click_share,
        segments_date,
        segments_device,
        business_id,
        year,
        month,
        day

    from source

)

select * from renamed
