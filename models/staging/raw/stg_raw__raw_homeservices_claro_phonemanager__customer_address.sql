with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__customer_address') }}

),

renamed as (

    select
        id,
        address_type_id,
        customer_id,
        cep,
        state,
        city_id,
        city,
        street,
        district,
        number,
        reference,
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
        hp,
        coverage_status_id,
        isp,
        network_4g,
        viability_oi,
        viability_claro,
        type_viability,
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
