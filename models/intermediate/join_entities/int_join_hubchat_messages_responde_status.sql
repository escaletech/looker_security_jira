with cte_join_msg as(
    select hubchat_chat_messages_id, desc_status_menssagem from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages') }}
    union all
    select hubchat_chat_messages_id, desc_status_menssagem from {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages') }}
)
,cte_join_ids as(
    SELECT 
        hubchat_chat_messages_id
        ,case when desc_status_menssagem like '%sent%' then 1 else 0 end flag_enviado
        ,case when desc_status_menssagem like '%delivered%' then 1 else 0 end flag_entregue
        ,case when desc_status_menssagem like '%read%' then 1 else 0 end flag_lido
    FROM cte_join_msg
)
select * from cte_join_ids