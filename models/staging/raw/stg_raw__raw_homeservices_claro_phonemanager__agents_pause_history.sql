with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__agents_pause_history') }}

),

renamed as (

    select
        id,
        user_id,
        agent_type_id,
        agent_action_id,
        time,
        agent,
        created_at,
        finalized_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
