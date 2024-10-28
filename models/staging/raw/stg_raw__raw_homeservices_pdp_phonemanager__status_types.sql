with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_phonemanager__status_types') }}

),

renamed as (

    select
        id,
        description,
        reference,
        pattern,
        parent,
        clean_status,
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
