with cte_join_tables as (
    select 
        mgs.* except(desc_digital_campaing,user_id)
        ,coalesce(ha.atendente_id,-1) as atendente_id
        ,htx.client_id
        ,coalesce(desc_digital_campaing,'SEM CAMPANHA') as digital_campaing_id
        ,coalesce(HC.valor_mgs,0) as valor_mgs
        ,ROW_NUMBER() OVER (PARTITION BY mgs.message_session_id ORDER BY tsp_message ASC) AS order_msg
    from {{ ref('int_join_hubchat_messages') }} mgs
    left join {{ ref('int_hubspot_cost') }} HC ON  HC.dt_cost = DATE_TRUNC('MONTH',mgs.tsp_message) and (mgs.flag_paid_msg = 1)
    left join {{ ref('int_join_hubchat_context') }} htx on htx.message_session_id = mgs.message_session_id
    left join {{ ref('int_join_hubchat_attendance') }} ha on ha.message_session_id = mgs.message_session_id
)
,cte_metrics as (
    select 
        hubchat_chat_messages_id
        ,vertical_id
        ,marca_id
        ,produto_id
        ,message_session_id
        ,atendente_id
        ,client_id
        ,digital_campaing_id
        ,date_format(tsp_message,'yyyyMMdd') data_id
        ,tsp_message
        ,desc_message_source
        ,desc_message_status
        ,desc_message_text
        ,response_type as desc_response_type
        ,options as desc_text_options
        ,wpp_body as desc_text_wpp_body
        ,response as desc_text_response
        ,hsm_template
        ,flag_timeout
        ,flag_paid_msg
        ,flag_first_msg
        ,flag_last_msg
        ,flag_first_user_msg
        ,flag_first_agent_msg
        ,flag_last_bot_msg
        ,valor_mgs as vlr_custo_menssagem
        ,order_msg as nr_order_msg
    from cte_join_tables
)
select * from cte_metrics