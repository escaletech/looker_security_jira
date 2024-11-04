with cte_join_msg as(
    select * from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages') }}
    union all
    select * from {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages') }}
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
        ,options
        ,wpp_body
        ,response
        ,case when tsp_message = tsp_first_msg then 1 else 0 end flag_first_msg
        ,case when tsp_message = tsp_last_msg then 1 else 0 end flag_last_msg
        ,flag_timeout
        ,desc_code_product
    from cte_join_msg cjm
    left join {{ ref('int_join_hubchat_workspace') }} w on w.token = cjm.token
)
select * from cte_join_ids