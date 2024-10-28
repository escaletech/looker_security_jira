with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__attendances_products') }}

),

renamed as (

    select
        id,
        attendance_id,
        category_id,
        product_id,
        attribute_name,
        number,
        group_attribute,
        value,
        value_name,
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
