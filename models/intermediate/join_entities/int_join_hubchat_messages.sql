with cte_join_msg as(
    select * from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages') }}
    union all
    select * from {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages') }}
)
,cte_join_ids as(
    select
        cjm.*
        ,w.* except(token)
        ,case when tsp_message = tsp_first_msg then 1 else 0 end flag_first_msg
        ,case when tsp_message = tsp_last_msg then 1 else 0 end flag_last_msg
    from cte_join_msg cjm
    left join {{ ref('int_join_hubchat_workspace') }} w on w.token = cjm.token
)
select * from cte_join_ids