with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__teams') }}

),

renamed as (

    select
        id,
        identification,
        description,
        agent_start,
        agent_end,
        unit_id,
        department_id,
        users_id,
        block_schedule,
        shift,
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
