-- homeservice oi
with cte_context as (
    SELECT
        c.*
        ,f.desc_agente_bot  
    FROM {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages_context') }} c
    left join {{ ref('int_join_hubchat_workspace') }} w on w.token = c.token
    left join {{ ref('dim_agente_bot') }} f on f.agente_bot_id = w.agente_bot_id
    where true
        and desc_agente_bot = 'HOMESERVICES- OI - VENDA'
)
,cte_calculated as(
SELECT
    c.message_session_id
    ,c.desc_agente_bot
    ,c.timestamp
    ,c.token
    ,1 conversas
    ,CASE WHEN c.welcome = 'true' THEN 1 ELSE 0 END interacao
    ,CASE WHEN c.welcome = 'true' AND c.customer IS NOT NULL AND c.qualification IS NOT NULL THEN 1 ELSE 0 END qualificacao
    ,CASE WHEN c.welcome = 'true' AND c.customer IS NOT NULL AND c.qualification = 'sale' THEN 1 ELSE 0 END qualificacao_compra
    ,CASE WHEN c.welcome = 'true' AND c.start_viability = 'true' THEN 1 ELSE 0 END endereco
    ,CASE WHEN c.welcome = 'true' AND c.start_viability = 'true' AND c.finish_viability = 'true' AND c.address_validate = 'true' THEN 1 ELSE 0 END viabilidade
    ,CASE WHEN c.welcome = 'true' AND c.start_viability = 'true' AND c.viability_product = 'true' AND c.viability_product_description  IN ('cabo', 'fibra') THEN 1 ELSE 0 END viabilidade_com_cobertura
    ,CASE WHEN c.welcome = 'true' AND c.start_viability = 'true' AND c.show_product = 'true' AND c.type_product = 'true' AND c.select_product = 'true' AND c.product_description IS NOT NULL THEN 1 ELSE 0 END pacote
    ,CASE WHEN c.welcome = 'true' AND c.start_checkout = 'true' THEN 1 ELSE 0 END checkout
    ,CASE WHEN c.welcome = 'true' AND c.start_checkout = 'true' AND c.finish_checkout = 'true' AND c.order = 'true' THEN 1 ELSE 0 END pedido
FROM cte_context AS c
LEFT JOIN {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages_from') }}  AS chat_messages_from ON chat_messages_from.message_session_id = c.message_session_id AND chat_messages_from.token = c.token
)
select 
    *
from cte_calculated