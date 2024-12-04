with cte_join_tables as (
    select 
        mgs.* except(desc_digital_campaing,user_id)
        ,coalesce(ha.atendente_id,-1) as atendente_id
        ,htx.phone_number
        ,htx.element_name_hsm as hsm
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
        hubchat_chat_messages_id as conversacional_id
        ,date_format(tsp_message,'yyyyMMdd') data_id
        ,vertical_id
        ,marca_id
        ,produto_id
        ,flowstep_id
        ,message_session_id
        ,atendente_id
        ,digital_campaing_id
        ,hsm
        ,tsp_message
        ,phone_number
        ,desc_message_source
        ,desc_message_status
        ,case when order_msg = 1 then hsm else null end desc_primeiro_hsm
        ,flag_enviado
        ,flag_entregue
        ,flag_lido
        ,flag_timeout
        ,flag_paid_msg
        ,flag_first_msg
        ,flag_last_msg
        ,flag_first_user_msg
        ,flag_first_agent_msg
        ,flag_last_bot_msg
        ,flag_deal
        ,valor_mgs as vlr_custo_menssagem
        --,vlr_tempo_resposta
        ,order_msg as nr_order_msg
    from cte_join_tables
)
select * from cte_metrics
where 
    vertical_id <> -1 
    or vertical_id is not null