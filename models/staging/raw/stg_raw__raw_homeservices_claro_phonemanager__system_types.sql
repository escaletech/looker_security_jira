with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__system_types') }}

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
        day

    from source

)

select * from renamed
