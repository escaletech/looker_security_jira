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
    phone_number,
    desc_primeiro_hsm,
    
    -- Seleciona a campanha e data, caso sejam constantes na sessão
    MIN(data_id) AS data_id,
    count(digital_campaing_id) AS digital_campaing_id,
    
    -- Agrega a timestamp para pegar a última mensagem da sessão
    MIN(tsp_message) AS tsp_first_message,
    MAX(tsp_message) AS tsp_last_message,
    
    -- Seleciona a última fonte e status da mensagem da sessão
    max(nr_order_msg) AS nr_order_msg,
    COUNT(desc_message_source) filter(where desc_message_source = 'user') AS qtde_user_msg,
    COUNT(desc_message_source) filter(where desc_message_source = 'api') AS qtde_api_msg,
    COUNT(desc_message_source) filter(where desc_message_source = 'hubchat') AS qtde_hubchat_msg,
    COUNT(desc_message_source) filter(where desc_message_source = 'agent') AS qtde_agent_msg,
    
    -- Agrega as flags para indicar se qualquer uma das mensagens na sessão tem essas características
    SUM(flag_timeout) AS qtde_timeout,
    SUM(flag_paid_msg) AS qtde_paid_msg,
   --SUM(flag_deal) AS qtde_deals,
    
    -- Soma o custo total de mensagens na sessão
    SUM(vlr_custo_menssagem) AS vlr_custo_menssagem

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