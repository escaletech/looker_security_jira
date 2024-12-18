with 

source as (

    select * from {{ source('trusted_homeservices_general', 'hubchat_chat_messages_from') }}

),

renamed as (

    select
        __v,
        _id,
        lake_created_at,
        origin,
        robot_id,
        session_init,
        timestamp,
        timestamp_init,
        token,
        user_agent,
        year,
        month,
        day,
        lake_key

    from source

)

select 
    session_init as message_session_id 
    ,timestamp_init
    ,token
 from renamed