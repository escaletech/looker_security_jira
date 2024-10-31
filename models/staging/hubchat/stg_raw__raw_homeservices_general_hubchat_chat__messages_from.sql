with 

source as (

    select * from {{ source('raw', 'raw_homeservices_general_hubchat_chat__messages_from') }}

),

renamed as (

    select
        __v,
        _id,
        lake_created_at,
        origin,
        session_init,
        timestamp,
        timestamp_init,
        token,
        user_agent,
        month,
        day

    from source

)

select * from renamed
