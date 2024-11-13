with 

source as (

    select * from {{ source('refined_homeservices_general', 'phonemanager_system_types') }}

),

renamed as (

    select
        id,
        description,
        reference,
        pattern,
        generic,
        add_by_rule,
        active,
        show,
        created_at,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day,
        lake_key,
        brand

    from source

)

select * from renamed
