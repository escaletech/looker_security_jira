with cte_join_msg as(
    select * from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages') }}
    union all
    select * from {{ ref('stg_trusted_homeservices_general__hubchat_escale_messages') }}
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
        ,response_type
        ,response_source
    from cte_join_msg cjm
    left join {{ ref('int_join_hubchat_workspace') }} w on w.token = cjm.token
)
select * from cte_join_ids