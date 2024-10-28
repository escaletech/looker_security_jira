with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_phonemanager__form_clicktocall') }}

),

renamed as (

    select
        id,
        token,
        field_name,
        field_value,
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
