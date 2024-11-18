with cte_join as(
    select message_session_id, phone_number, element_name_hsm, history_hsm from {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages_context') }}
    union all
    select message_session_id, phone_number, element_name_hsm, history_hsm from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_context') }}
)
select 
    message_session_id
    ,coalesce(phone_number,-1) as phone_number
    ,element_name_hsm
    ,history_hsm
from cte_join