with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_phonemanager__customer_address') }}

),

renamed as (

    select
        id,
        customer_id,
        street,
        number,
        complement_type,
        complement,
        complement2_type,
        complement2,
        complement3_type,
        complement3,
        complement4_type,
        complement4,
        complement5_type,
        complement5,
        district,
        reference,
        hp,
        cep,
        state,
        city_id,
        city,
        address_type_id,
        isp,
        network_4g,
        viability_oi,
        viability_claro,
        type_viability,
        value_stream,
        created_at,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
