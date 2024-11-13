with 

source as (

    select * from {{ source('refined_homeservices_general', 'phonemanager_attributes') }}

),

renamed as (

    select
        id,
        category_id,
        product_id,
        route,
        route_description,
        name,
        v2_name,
        title,
        group,
        type,
        class,
        required,
        php,
        html,
        col,
        reference_table_from,
        reference_column_from,
        reference_columnname_from,
        reference_params_from,
        reference_table_to,
        reference_column_to,
        reference_type,
        modules,
        value,
        section_types_id,
        group_inputs_id,
        sequence,
        filter,
        show,
        upd,
        edit,
        crm,
        active,
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

select 
    brand
    ,name
    ,group 
from renamed
