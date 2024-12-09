with cte_group as (
SELECT
    message_session_id,
    
    -- Seleciona o valor de `vertical_id`, `marca_id` e `produto_id` se for constante na sessão
    vertical_id,
    marca_id,
    produto_id,
    flowstep_id,
    
    -- Seleciona o primeiro `atendente_id` e `client_id` encontrados na sessão
    atendente_id,
    MIN(data_id) AS primeira_msg_data_id,
    MAX(data_id) AS ultima_msg_data_id,
    phone_number,
    min(desc_primeiro_hsm) desc_primeiro_hsm,
    
    -- Seleciona a campanha e data, caso sejam constantes na sessão
    
    count(digital_campaing_id) AS digital_campaing_id,
    
    -- Agrega a timestamp para pegar a última mensagem da sessão
    MIN(tsp_message) AS tsp_first_message,
    MAX(tsp_message) AS tsp_last_message,
    
    --flags
    sum(flag_abandono) flag_abandonou_chat,
    MAX(case when desc_message_source = 'user' then 1 else 0 end) AS flag_msg_usuario,
    MAX(case when desc_message_source = 'api' then 1 else 0 end) AS flag_msg_api,
    MAX(case when desc_message_source = 'hubchat' then 1 else 0 end) AS flag_msg_bot,
    MAX(case when desc_message_source = 'agent' then 1 else 0 end) AS flag_msg_agente,

    -- Seleciona a última fonte e status da mensagem da sessão
    COUNT(conversacional_id) AS qtde_msg_total,
    sum(flag_enviado) qtde_msg_enviadas,
    sum(flag_entregue) qtde_msg_entregues,
    sum(flag_lido) qtde_msg_lidas,
    COUNT(desc_message_source) filter(where desc_message_source = 'user') AS qtde_user_msg,
    COUNT(desc_message_source) filter(where desc_message_source = 'api') AS qtde_api_msg,
    COUNT(desc_message_source) filter(where desc_message_source = 'hubchat') AS qtde_hubchat_msg,
    COUNT(desc_message_source) filter(where desc_message_source = 'agent') AS qtde_agent_msg,
    
    -- Agrega as flags para indicar se qualquer uma das mensagens na sessão tem essas características
    sum(flag_transbordo) AS qtde_transboardo,
    SUM(flag_timeout) AS qtde_timeout,
    SUM(flag_paid_msg) AS qtde_paid_msg,
   --SUM(flag_deal) AS qtde_deals,

    timestampdiff(minute,min(tsp_message),max(tsp_message)) tempo_conversa_total,
    timestampdiff(minute,min(tsp_entregue),min(tsp_lido)) tempo_prmeira_leitura,
    
    -- Soma o custo total de mensagens na sessão
    SUM(vlr_custo_menssagem) AS vlr_custo_menssagem,
    SUM(vlr_custo_menssagem) filter(where flag_timeout = 1) AS vlr_custo_menssagem_timeout

--, sum(case when flag_paid_msg = 1 and flag_entregue = 1 then 1 else 0 end) qtde_entregues_pagos -- Os pagos são dados por msg vindas de usuário
--, sum(case when flag_paid_msg = 1 and flag_entregue = 0 then 1 else 0 end) qtde_nao_entregues_pagos
--, sum(case when flag_paid_msg = 1 and flag_entregue = 1 then vlr_custo_menssagem else 0 end) vlr_entregues_pagos
--, sum(case when flag_paid_msg = 1 and flag_entregue = 0 then vlr_custo_menssagem else 0 end) vlr_nao_entregues_pagos
--, sum(case when flag_paid_msg = 1 and flag_timeout = 1 then vlr_custo_menssagem else 0 end) vlr_custo_timeout

FROM
    {{ ref('ft_conversacional') }}
GROUP BY
    all
)
,cte_hsm as(
    select
        message_session_id
        ,count(hsm) qtde_hsm
    from {{ ref('ft_hsm_history') }}
    group by 1
)
, cte_join_deals as (
    select 
        g.*
        ,d.* except(message_session_id)
        ,coalesce(hsm.qtde_hsm,0) as qtde_hsm
    from cte_group g
    left join {{ ref('int_join_hubspot_session_msg_to_deal') }} d on d.message_session_id = g.message_session_id
    left join cte_hsm hsm on hsm.message_session_id = g.message_session_id
)
select * from cte_join_deals