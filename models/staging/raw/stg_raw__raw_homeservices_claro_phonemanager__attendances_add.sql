with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__attendances_add') }}

),

renamed as (

    select
        id,
        attendance_id,
        type,
        description,
        name_field,
        value,
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
