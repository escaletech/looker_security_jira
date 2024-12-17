SELECT
    message_session_id,
    desc_agente_bot,
    welcome,
    deseja_prosseguir,
    simulacao,
    dados_pessoais,
    dados_endereco AS endereco,
    transferencia_consultor_geral as pedido
FROM {{ ref('dim_funil_produto_a') }}

UNION ALL

SELECT
    message_session_id,
    desc_agente_bot,
    conversas AS welcome,
    identificacao AS deseja_prosseguir,
    simulacao,
    dados_pessoais,
    endereco_tel_cliente AS endereco,
    endereco_tel_cliente as pedido
FROM {{ ref('dim_funil_produto_b') }}

UNION ALL

SELECT
    message_session_id,
    desc_agente_bot,
    conversas AS welcome,
    identificacao AS deseja_prosseguir,
    simulacao,
    dados_pessoais,
    endereco_tel_cliente AS endereco,
    endereco_tel_cliente as pedido
FROM {{ ref('dim_funil_produto_c') }}

UNION ALL

SELECT
    message_session_id,
    desc_agente_bot,
    conversas AS welcome,
    interacao AS deseja_prosseguir,
    NULL AS simulacao,
    NULL AS dados_pessoais,
    endereco AS endereco,
    pedido as pedido
FROM {{ ref('dim_funil_produto_d') }}

UNION ALL

SELECT
    message_session_id,
    desc_agente_bot,
    welcome,
    identificacao_cpf AS deseja_prosseguir,
    simulacao_elegibilidade AS simulacao,
    dados_pessoais_email AS dados_pessoais,
    endereco_cep AS endereco,
    nro_da_conta as pedido
FROM {{ ref('dim_funil_produto_e') }}

UNION ALL

SELECT
    message_session_id,
    desc_agente_bot,
    conversas AS welcome,
    identificacao AS deseja_prosseguir,
    simulacao,
    dados_pessoais,
    endereco_e_tel_do_cliente AS endereco,
    nro_endereco as pedido
FROM {{ ref('dim_funil_produto_f') }}
