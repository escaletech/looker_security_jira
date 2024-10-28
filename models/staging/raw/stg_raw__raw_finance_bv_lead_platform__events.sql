with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_lead_platform__events') }}

),

renamed as (

    select
        _id,
        action,
        created_at,
        lake_created_at,
        lead_id,
        tracking_id,
        year,
        month,
        day

    from source

)

select * from renamed
