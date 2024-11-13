--FINANCIMENTO BV
with cte_context as (
    SELECT
        c.*       
        ,f.desc_flowstep  
    FROM {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_context') }} c
    left join {{ ref('int_join_hubchat_workspace') }} w on w.token = c.token
    left join {{ ref('dim_flowstep') }} f on f.flowstep_id = w.flowstep_id
    where true
        and welcome = 'true'
        and desc_flowstep = 'FINANCE - BV - FINANCIAMENTO'
)
, cte_calculated as (
SELECT    
    c.message_session_id
    ,c.desc_flowstep
    ,c.timestamp
    ,c.token
    ,1 as conversas
    ,CASE WHEN c.individual_registration = 'true' THEN 1  ELSE NULL END identificacao
    ,CASE WHEN c.opt_out = 'true' THEN 1  ELSE NULL END nao_quer_contato
    ,CASE WHEN c.opt_out IS NULL THEN 1  ELSE NULL END deseja_prosseguir
    ,CASE WHEN c.name = 'true' THEN 1  ELSE NULL END nome
    ,CASE WHEN c.email = 'true' THEN 1  ELSE NULL END email
    ,CASE WHEN c.individual_registration = 'true' THEN 1  ELSE NULL END cpf
    ,CASE WHEN c.product_deal_stage_description IS NOT NULL THEN 1  ELSE NULL END segmentacao_de_compra
    ,CASE WHEN c.type_product_description IS NOT NULL THEN 1  ELSE NULL END tipo_de_veiculo
    ,CASE WHEN c.product_deal_stage_description IS NOT NULL THEN 1  ELSE NULL END escolha_do_veiculo
    ,CASE WHEN c.product_deal_stage_description LIKE 'negociando_veiculo' THEN 1  ELSE NULL END negociando_veiculo
    ,CASE WHEN c.product_deal_stage_description LIKE 'negociando_veiculo' AND c.purchase_time IS NOT NULL THEN 1  ELSE NULL END periodo_de_compra
    ,CASE WHEN c.name = 'true' AND c.email = 'true' AND c.individual_registration = 'true' AND c.product_deal_stage_description LIKE 'pesquisando_onde_comprar' THEN 1  ELSE NULL END pesquisando_onde_comprar
    ,CASE WHEN c.product_deal_stage_description LIKE 'pesquisando_onde_comprar' AND c.purchase_situation_description IS NOT NULL THEN 1  ELSE NULL END momento_da_compra
    ,CASE WHEN c.product_deal_stage_description LIKE 'pesquisando_onde_comprar' AND c.purchase_situation_description LIKE 'mais_rapido_possivel' THEN 1  ELSE NULL END mais_rapido_possivel
    ,CASE WHEN c.product_deal_stage_description LIKE 'pesquisando_onde_comprar' AND c.purchase_situation_description LIKE 'melhor_opcao' THEN 1  ELSE NULL END melhor_opcao
    ,CASE WHEN c.product_deal_stage_description = 'refinanciamento' THEN 1  ELSE NULL END refinanciamento
    ,CASE WHEN c.product_confirmation = 'true' THEN 1  ELSE NULL END detalhamento_veiculo
    ,CASE WHEN c.product_brand IS NOT NULL THEN 1  ELSE NULL END marca
    ,CASE WHEN c.product_year = 'true' THEN 1  ELSE NULL END ano
    ,CASE WHEN c.product_model IS NOT NULL THEN 1  ELSE NULL END versao_do_veiculo
    ,CASE WHEN c.product_km = 'true' THEN 1  ELSE NULL END km
    ,CASE WHEN c.product_value IS NOT NULL THEN 1  ELSE NULL END valor_de_venda
    ,CASE WHEN c.birth_date = 'true' THEN 1  ELSE NULL END data_de_nascimento
    ,CASE WHEN c.product_state_of_origin = 'true' THEN 1  ELSE NULL END uf_licenciamento
    ,CASE WHEN c.product_confirmation = 'true' THEN 1  ELSE NULL END confirmacao_do_veiculo
    ,CASE WHEN c.down_payment_type IS NOT NULL THEN 1  ELSE NULL END simulacao
    ,CASE WHEN c.down_payment IS NOT NULL THEN 1  ELSE NULL END descricao_da_entrada
    ,CASE WHEN c.pre_simulate = 'true' THEN 1  ELSE NULL END api_pre_simulacao
    ,CASE WHEN c.simulation_product = 'true' THEN 1  ELSE NULL END api_simulacao
    ,CASE WHEN c.simulation_eligibility = 'Aprovado' THEN 1  ELSE NULL END aprovado
    ,CASE WHEN c.simulation_eligibility = 'Reprovado' THEN 1  ELSE NULL END reprovado
    ,CASE WHEN c.simulation_product = 'true' AND c.simulation_eligibility = 'Não foi possível calcular' THEN 1  ELSE NULL END nao_foi_possivel_calcular
    ,CASE WHEN c.down_payment_type IS NOT NULL THEN 1  ELSE NULL END validacao_do_valor_de_entrada
    ,CASE WHEN c.down_payment_type = 'Entrada Mínima' THEN 1  ELSE NULL END entrada_minima
    ,CASE WHEN c.down_payment_type = 'Entrada Maior' THEN 1  ELSE NULL END entrada_maior
    ,CASE WHEN c.down_payment_type = 'Entrada abaixo da mínima' THEN 1  ELSE NULL END entrada_abaixo_da_minima
    ,CASE WHEN c.show_product = 'true' THEN 1  ELSE NULL END proposta
    ,CASE WHEN c.show_product IS NOT NULL THEN 1  ELSE NULL END resumo_da_proposta
    ,CASE WHEN c.gender IS NOT NULL THEN 1  ELSE NULL END dados_pessoais
    ,CASE WHEN c.marital_status IS NOT NULL THEN 1  ELSE NULL END estado_civil
    ,CASE WHEN c.nationality IS NOT NULL THEN 1  ELSE NULL END nacionalidade
    ,CASE WHEN c.city_of_origin IS NOT NULL THEN 1  ELSE NULL END cidade_de_nascimento
    ,CASE WHEN c.state_of_origin IS NOT NULL THEN 1  ELSE NULL END estado_de_nascimento
    ,CASE WHEN c.mother_name IS NOT NULL THEN 1  ELSE NULL END nome_da_mae
    ,CASE WHEN c.national_identity IS NOT NULL THEN 1  ELSE NULL END rg
    ,CASE WHEN c.issuer_national_identity IS NOT NULL THEN 1  ELSE NULL END orgao_emissor_rg
    ,CASE WHEN c.issue_state_national_identity IS NOT NULL THEN 1  ELSE NULL END estado_emissor_rg
    ,CASE WHEN c.issue_date_national_identity IS NOT NULL THEN 1  ELSE NULL END data_de_emissao_rg
    ,CASE WHEN c.gender IS NOT NULL THEN 1  ELSE NULL END genero
    ,CASE WHEN c.income_value IS NOT NULL THEN 1  ELSE NULL END ocupacao_e_renda
    ,CASE WHEN c.employer_type IS NOT NULL THEN 1  ELSE NULL END fonte_pagadora
    ,CASE WHEN c.employer_type IS NOT NULL AND c.occupation IS NOT NULL THEN 1  ELSE NULL END tipo_profissional
    ,CASE WHEN c.pension_number IS NOT NULL THEN 1  ELSE NULL END numero_do_beneficio_inss
    ,CASE WHEN c.federal_tax_identification_number IS NOT NULL THEN 1  ELSE NULL END cnpj
    ,CASE WHEN c.zipcode_cnpj IS NOT NULL THEN 1  ELSE NULL END cep_cnpj
    ,CASE WHEN c.federal_tax_identification_number IS NOT NULL AND c.number_cnpj IS NOT NULL THEN 1  ELSE NULL END nro_endereco_cnpj
    ,CASE WHEN c.employment_time IS NOT NULL THEN 1  ELSE NULL END tempo_de_ocupacao
    ,CASE WHEN c.income_value IS NOT NULL THEN 1  ELSE NULL END renda_atual
    ,CASE WHEN c.address_number IS NOT NULL THEN 1  ELSE NULL END endereco_e_tel_do_cliente
    ,CASE WHEN c.address_zip_code IS NOT NULL THEN 1  ELSE NULL END cep
    ,CASE WHEN c.address_number IS NOT NULL THEN 1  ELSE NULL END nro_endereco
    --,CASE WHEN general_hubchat_attendance.timestamp_init   IS NOT NULL THEN 1  ELSE NULL END general_hubchat_from_total_conversations_transfer_funil_geral
FROM cte_context c
    join {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_from') }} f on f.message_session_id = c.message_session_id
    --join {{ ref('stg_trusted_finance_general__hubchat_escale_finance_attendance') }} a on a.message_session_id = c.message_session_id
 )
select
    *
from cte_calculated c