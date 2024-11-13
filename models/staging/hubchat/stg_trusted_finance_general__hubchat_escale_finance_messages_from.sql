with 

source as (

    select * from {{ source('trusted_finance_general', 'hubchat_escale_finance_messages_from') }}

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
        year,
        month,
        day,
        lake_key

    from source

)

select 
    session_init as message_session_id 
    ,timestamp_init
from renamed
