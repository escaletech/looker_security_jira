with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_lead_platform__crm_invalid_fields') }}

),

renamed as (

    select
        _id,
        category,
        created_at,
        event_id,
        lake_created_at,
        lead_id,
        name,
        reason,
        value,
        year,
        month,
        day

    from source

)

select * from renamed
