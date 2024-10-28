with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_phonemanager__action_type') }}

),

renamed as (

    select
        id,
        name,
        color,
        pbx_code_id,
        type,
        time_limit,
        attendances_limit,
        user_block,
        modal_fixed,
        show,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
