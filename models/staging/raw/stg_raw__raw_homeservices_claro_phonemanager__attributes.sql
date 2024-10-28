with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__attributes') }}

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
        day

    from source

)

select * from renamed
