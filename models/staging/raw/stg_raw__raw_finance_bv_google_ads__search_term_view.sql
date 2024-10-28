with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_google_ads__search_term_view') }}

),

renamed as (

    select
        ad_group_ad_ad,
        ad_group_id,
        ad_group_name,
        campaign_id,
        campaign_name,
        customer_id,
        lake_created_at,
        segments_date,
        business_id,
        year,
        month,
        day

    from source

)

select * from renamed
