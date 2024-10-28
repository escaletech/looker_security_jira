with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_orderprocessing_nozhka') }}

),

renamed as (

    select
        _id,
        accept_provider_contact,
        accept_terms_digital_account,
        accept_terms_general,
        accept_terms_loyalty,
        campaign,
        charged_price,
        contract_type,
        created_at,
        created_at_last_event,
        error_last_event,
        is_migration,
        is_portability,
        is_valid,
        kind,
        lake_created_at,
        medium,
        message_last_event,
        name,
        name_last_event,
        number,
        order,
        price,
        provider,
        provider_id,
        sale_id,
        seasson_id,
        sku,
        source,
        source_last_event,
        updated_at,
        year,
        month,
        day

    from source

)

select * from renamed
