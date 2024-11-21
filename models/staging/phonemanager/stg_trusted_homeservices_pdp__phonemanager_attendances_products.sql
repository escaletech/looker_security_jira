with 

source as (

    select * from {{ source('trusted_homeservices_pdp', 'phonemanager_attendances_products') }}

),

renamed as (

    select
        id,
        attendance_id,
        category_id,
        product_id,
        attribute_name,
        group_attribute,
        number,
        value,
        value_name,
        created_at,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day,
        lake_key

    from source

)

select 
    attendance_id 
    ,group_attribute
from renamed
