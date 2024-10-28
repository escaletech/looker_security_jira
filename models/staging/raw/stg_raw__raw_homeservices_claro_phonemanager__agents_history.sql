with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__agents_history') }}

),

renamed as (

    select
        id,
        user_id,
        agent,
        peer,
        team_id,
        action_user_id,
        peer_type_id,
        agent_type_id,
        agent_action_id,
        time,
        lock,
        time_interval,
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
