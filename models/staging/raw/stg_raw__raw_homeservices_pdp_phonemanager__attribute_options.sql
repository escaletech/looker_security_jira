with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_phonemanager__attribute_options') }}

),

renamed as (

    select
        id,
        attribute_name,
        name,
        catalog_id,
        description,
        pontos,
        required,
        active,
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
