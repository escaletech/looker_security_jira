--02.01. [BV - FINANC] Funil Outbound Aut
with cte_context as (
    SELECT
        c.*         
    FROM {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_context') }} c
    left join {{ ref('int_join_hubchat_workspace') }} w on w.token = c.token
    left join {{ ref('dim_flowstep') }} f on f.flowstep_id = w.flowstep_id
    where true
        and type_contact = 'outbound'
        and desc_flowstep = 'FINANCE - BV - INBOUND/OUTBOUND AUTO'
)
, cte_calculated as (
SELECT
    c.message_session_id
    ,c.timestamp
    ,1 AS conversas
    ,CASE WHEN welcome = 'true' THEN 1 ELSE 0 END interacao
    ,CASE WHEN welcome = 'true' and product_deal_stage_description IS NOT NULL THEN 1 ELSE 0 END segmento_de_compra
    ,CASE WHEN welcome = 'true' and type_seller_description IS NOT NULL THEN 1 ELSE 0 END tipo_compra
    ,CASE WHEN welcome = 'true' and eligible_product IS NOT NULL THEN 1 ELSE 0 END pre_analise
    ,CASE WHEN welcome = 'true' and product_confirmation = 'true' THEN 1 ELSE 0 END detalhamento_veiculo
    ,CASE WHEN welcome = 'true' and down_payment_type IS NOT NULL THEN 1 ELSE 0 END simulacao
    ,CASE WHEN welcome = 'true' and show_product = 'true' THEN 1 ELSE 0 END proposta
    ,CASE WHEN welcome = 'true' and gender IS NOT NULL THEN 1 ELSE 0 END dados_pessoais
    ,CASE WHEN welcome = 'true' and income_value IS NOT NULL THEN 1 ELSE 0 END ocupacao_renda
    ,CASE WHEN welcome = 'true' and address_number IS NOT NULL THEN 1 ELSE 0 END endereco_tel_cliente

FROM cte_context c
    join {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_from') }} f on f.message_session_id = c.message_session_id
)
select
    *
from cte_calculated c


--valores proximos mas não bateram