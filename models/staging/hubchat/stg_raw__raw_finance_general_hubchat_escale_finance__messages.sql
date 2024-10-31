with 

source as (

    select * from {{ source('raw', 'raw_finance_general_hubchat_escale_finance__messages') }}

),

renamed as (

    select
        __v,
        _id,
        data,
        lake_created_at,
        message_status,
        session_init,
        text,
        timestamp,
        token,
        user_id,
        who,
        year,
        month,
        day

    from source

)

select
    session_init message_session_id
    ,token
    ,user_id
    ,timestamp tsp_message
    ,who desc_message_source
    ,message_status desc_message_status
    ,text desc_message_text
from renamed
