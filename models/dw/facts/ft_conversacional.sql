with cte_join_tables as (
    select 
        mgs.* except(desc_digital_campaing,user_id)
        ,coalesce(ha.atendente_id,-1) as atendente_id
        ,htx.phone_number
        ,htx.element_name_hsm as hsm
        ,coalesce(desc_digital_campaing,'SEM CAMPANHA') as digital_campaing_id
        ,coalesce(HC.valor_mgs,0) as valor_mgs
    from {{ ref('int_join_hubchat_messages') }} mgs
    left join {{ ref('int_hubspot_cost') }} HC ON  HC.dt_cost = DATE_TRUNC('MONTH',mgs.tsp_message) and (mgs.flag_paid_msg = 1)
    left join {{ ref('int_join_hubchat_context') }} htx on htx.message_session_id = mgs.message_session_id
    left join {{ ref('int_join_hubchat_attendance') }} ha on ha.message_session_id = mgs.message_session_id
)
,cte_metrics as (
    select 
        hubchat_chat_messages_id as conversacional_id
        ,date_format(tsp_message,'yyyyMMdd') data_id
        ,hour_id hora_id
        ,vertical_id
        ,marca_id
        ,produto_id
        ,agente_bot_id
        ,message_session_id
        ,atendente_id
        ,digital_campaing_id
        ,hsm
        ,tsp_message
        ,tsp_enviado
        ,tsp_entregue
        ,tsp_lido
        ,phone_number
        ,desc_message_source
        ,desc_message_status
        ,case when order_msg = 1 then hsm else null end desc_primeiro_hsm
        ,flag_inbound
        ,flag_outbound
        ,flag_enviado
        ,flag_entregue
        ,flag_lido
        ,flag_timeout
        ,flag_transbordo
        ,flag_paid_msg
        ,flag_first_msg
        ,flag_last_msg
        ,flag_first_user_msg
        ,flag_first_agent_msg
        ,flag_last_bot_msg
        ,flag_deal
        ,flag_abandono
        ,valor_mgs as vlr_custo_menssagem
        ,order_msg as nr_order_msg
    from cte_join_tables
)
select * from cte_metrics
where 
    vertical_id <> -1 
    or vertical_id is not null