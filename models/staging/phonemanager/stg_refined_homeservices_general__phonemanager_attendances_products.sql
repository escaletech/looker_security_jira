with 

source as (

    select * from {{ source('refined_homeservices_general', 'phonemanager_attendances_products') }}

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
        day,
        lake_key,
        brand

    from source

)

select * from renamed
