select 
    tm.* 
    ,ts.* except(message_session_id)
from {{ ref('stg_trusted_cross_general__typebot_message') }} tm
left join {{ ref('stg_trusted_cross_general__typebot_session') }} ts on ts.message_session_id = tm.message_session_id