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
    client_id,
    
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
    
    -- Seleciona o template HSM e o tipo de resposta (se constantes na sessão)
    count(hsm_template) filter(where hsm_template <> 'SEM CAMPANHA') AS hsm_template,
    
    -- Agrega as flags para indicar se qualquer uma das mensagens na sessão tem essas características
    SUM(flag_timeout) AS qtde_timeout,
    SUM(flag_paid_msg) AS qtde_paid_msg,
    
    -- Soma o custo total de mensagens na sessão
    SUM(vlr_custo_menssagem) AS vlr_custo_menssagem

FROM
    {{ ref('ft_conversacional') }}
GROUP BY
    all
)
select * from cte_group