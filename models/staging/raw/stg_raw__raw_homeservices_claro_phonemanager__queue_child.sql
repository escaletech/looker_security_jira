with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__queue_child') }}

),

renamed as (

    select
        id,
        queue_id,
        child,
        queue_group_types_id,
        music,
        context,
        priority,
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
