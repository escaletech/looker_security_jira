with 

source as (

    select * from {{ source('raw', 'raw_homeservices_general_hubchat_chat__messages') }}

),

renamed as (

    select
        __v,
        _id,
        data,
        lake_created_at,
        session_init,
        text,
        timestamp,
        token,
        who,
        month,
        day

    from source

)

select
    session_init message_session_id
    ,token
    ,'' user_id
    ,timestamp tsp_message
    ,who desc_message_source
    ,'' desc_message_status
    ,text desc_message_text
from renamed
