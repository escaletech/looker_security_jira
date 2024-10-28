with 

source as (

    select * from {{ source('raw', 'raw_finance_itau_hubspot__property_values') }}

),

renamed as (

    select
        archived,
        created_at,
        created_user_id,
        description,
        field_type,
        group_name,
        has_unique_value,
        hidden,
        label,
        lake_created_at,
        name,
        origin,
        type,
        updated_at,
        updated_user_id,
        option_description,
        option_display_order,
        option_hidden,
        option_label,
        option_value,
        month,
        day

    from source

)

select * from renamed
