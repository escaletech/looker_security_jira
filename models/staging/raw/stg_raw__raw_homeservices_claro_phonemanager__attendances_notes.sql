with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__attendances_notes') }}

),

renamed as (

    select
        id,
        attendance_id,
        user_id,
        types_id,
        product_id,
        description,
        created_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
