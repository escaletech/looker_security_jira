with 

source as (

    select * from {{ source('raw', 'raw_finance_itau_hubspot__associations') }}

),

renamed as (

    select
        lake_created_at,
        contact_id,
        contact_type,
        from_id,
        month,
        day

    from source

)

select * from renamed
