    
with cte_explode_finance as(
    select 
        hubchat_chat_messages_id
        , EXPLODE(desc_status_menssagem) desc_status_menssagem
    from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages') }}
)
, cte_build_finance as(
  SELECT
    hubchat_chat_messages_id,
    TRIM(SPLIT(desc_status_menssagem, ',')[1]) AS tipo,
    TRIM(SPLIT(SPLIT(desc_status_menssagem, ',')[2], '}')[0])::int::timestamp timestamp
  FROM cte_explode_finance
)
,cte_group_finance as(
    select 
        hubchat_chat_messages_id
        ,max(case when tipo = 'sent' then 1 else 0 end) flag_enviado
        ,max(case when tipo = 'delivered' then 1 else 0 end) flag_entregue
        ,max(case when tipo = 'read' then 1 else 0 end) flag_lido
        ,min(case when tipo = 'sent' then timestamp else null end) tsp_enviado
        ,min(case when tipo = 'delivered' then timestamp else null end) tsp_entregue
        ,min(case when tipo = 'read' then timestamp else null end) tsp_lido
    from cte_build_finance
    group by 1
)
,cte_homeservices as(
    select hubchat_chat_messages_id, desc_status_menssagem from {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages') }}
)
,cte_explode as(
    select
        hubchat_chat_messages_id
        ,TRIM(exploded.tipo) tipo
        ,CAST(exploded.timestamp AS timestamp) AS timestamp
    from cte_homeservices
    LATERAL VIEW EXPLODE(desc_status_menssagem) AS exploded
)
,cte_group_homeservices as(
    select 
        hubchat_chat_messages_id
        ,max(case when tipo = 'sent' then 1 else 0 end) flag_enviado
        ,max(case when tipo = 'delivered' then 1 else 0 end) flag_entregue
        ,max(case when tipo = 'read' then 1 else 0 end) flag_lido
        ,min(case when tipo = 'sent' then timestamp else null end) tsp_enviado
        ,min(case when tipo = 'delivered' then timestamp else null end) tsp_entregue
        ,min(case when tipo = 'read' then timestamp else null end) tsp_lido
    from cte_explode
    group by 1
)
,cte_join_msg as(
    select * from cte_group_homeservices
    union all
    select * from cte_group_finance
)
select * from cte_join_msg