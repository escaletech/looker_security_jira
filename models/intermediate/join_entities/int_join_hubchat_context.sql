with cte_join as(
    select message_session_id, client_id from {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages_context') }}
    union all
    select message_session_id, client_id from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_context') }}
)
select 
    message_session_id
    ,coalesce(client_id,-1) as client_id
from cte_join