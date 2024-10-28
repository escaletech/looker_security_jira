with 

source as (

    select * from {{ source('raw', 'raw_finance_itau_hubspot__archived_deals') }}

),

renamed as (

    select
        archived,
        archived_at,
        created_at,
        id,
        lake_created_at,
        updated_at,
        properties_amount,
        properties_closedate,
        properties_createdate,
        properties_dealname,
        properties_dealstage,
        properties_hs_lastmodifieddate,
        properties_pipeline,
        month,
        day

    from source

)

select * from renamed
