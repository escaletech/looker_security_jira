with cte_join_msg as(
    select * except(desc_status_menssagem) from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages') }}
    union all
    select * except(desc_status_menssagem) from {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages') }}
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
        ,case when desc_message_source = 'agent' and lag(flag_timeout) over(partition by cte_join_ids.message_session_id order by tsp_message) = 1 then 1 else 0 end flag_transbordo
        ,case when tsp_last_msg = tsp_message and std.message_session_id is not null then 1 else 0 end flag_deal
        ,case when flag_last_msg = 1 and flag_timeout = 1 then 1 else 0 end flag_abandono
        ,date_format(tsp_message, 'HHmm') AS hour_id
    from cte_join_ids
    left join {{ ref('int_join_hubspot_session_msg_to_deal') }} std on std.message_session_id = cte_join_ids.message_session_id
)
, cte_reponse_status as(
    select 
        cte_deal.*
        ,mrs.* except(hubchat_chat_messages_id)
    from cte_deal
    left join {{ ref('int_join_hubchat_messages_responde_status') }} mrs on mrs.hubchat_chat_messages_id = cte_deal.hubchat_chat_messages_id
)
select * from cte_reponse_status

