with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_hubspot__archived_contacts') }}

),

renamed as (

    select
        archived,
        archived_at,
        created_at,
        id,
        lake_created_at,
        updated_at,
        properties_createdate,
        properties_email,
        properties_firstname,
        properties_hs_object_id,
        properties_lastmodifieddate,
        properties_lastname,
        year,
        month,
        day

    from source

)

select * from renamed
