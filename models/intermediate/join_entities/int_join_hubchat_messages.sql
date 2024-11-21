with cte_join_msg as(
    select * from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages') }}
    union all
    select * from {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages') }}
)
,cte_join_ids as(
    select
        cjm.* --except(token)
        ,w.* except(token)
        ,case when tsp_message = tsp_first_msg then 1 else 0 end flag_first_msg
        ,case when tsp_message = tsp_last_msg then 1 else 0 end flag_last_msg
        ,case when tsp_message = tsp_first_user_msg then 1 else 0 end flag_first_user_msg
        ,case when tsp_message = tsp_first_agent_msg then 1 else 0 end flag_first_agent_msg
        ,case when tsp_message = tsp_last_bot_msg then 1 else 0 end flag_last_bot_msg
from cte_join_msg cjm
    left join {{ ref('int_join_hubchat_workspace') }} w on w.token = cjm.token
)
, cte_deal as(
    select 
        cte_join_ids.*
        ,case when tsp_last_msg = tsp_message and std.message_session_id is not null then 1 else 0 end flag_deal
    from cte_join_ids
    left join {{ ref('int_join_session_msg_to_deal') }} std on std.message_session_id = cte_join_ids.message_session_id
)
select * from cte_deal