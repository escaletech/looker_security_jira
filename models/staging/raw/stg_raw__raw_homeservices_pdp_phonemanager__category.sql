with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_phonemanager__category') }}

),

renamed as (

    select
        id,
        description,
        reference,
        color,
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
