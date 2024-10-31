with cte_join_msg as(
    select * from {{ ref('stg_raw__raw_homeservices_general_hubchat_chat__messages') }}
    union all
    select * from {{ ref('stg_raw__raw_finance_general_hubchat_escale_finance__messages') }}
)
,cte_join_ids as(
    select
        message_session_id
        ,vertical_id
        ,marca_id
        ,produto_id
        ,tsp_message
        ,desc_message_source
        ,desc_message_status
        ,desc_message_text
    from cte_join_msg cjm
    left join {{ ref('int_join_hubchat_workspace') }} w on w.token = cjm.token
)
select distinct desc_message_source from cte_join_ids