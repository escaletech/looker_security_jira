--[ITAU - Consig] Funil Conversacional


with cte_context (
    SELECT
        c.*
        ,f.desc_agente_bot
    FROM {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_context') }} c
    left join {{ ref('int_join_hubchat_workspace') }} w on w.token = c.token
    left join {{ ref('dim_agente_bot') }} f on f.agente_bot_id = w.agente_bot_id
    where welcome = 'true'
        and desc_agente_bot = 'FINANCE - ITAU - CONSIGNADO'

)
, cte_calculated as (
SELECT
    c.message_session_id
    ,c.desc_agente_bot
    ,f.timestamp_init
    ,1 AS welcome
    ,CASE WHEN opt_out = 'true' THEN 1 ELSE 0 END nao_quer_contato
    ,CASE WHEN opt_out IS NULL THEN 1 ELSE 0 END deseja_prosseguir
    ,CASE WHEN unlock_inss IS NOT NULL THEN 1 ELSE 0 END tempestividade
    ,CASE WHEN product_description IS NOT NULL THEN 1 ELSE 0 END simulacao
    ,CASE WHEN product_description IS NOT NULL THEN 1 ELSE 0 END opcoes_emprestivo
    ,CASE WHEN simulation_product IS NOT NULL THEN 1 ELSE 0 END simulacao_dos_valores
    ,CASE WHEN show_product IS NOT NULL THEN 1 ELSE 0 END selecao_valor
    ,CASE WHEN loan_amount IS NOT NULL THEN 1 ELSE 0 END fonte_renda
    ,CASE WHEN show_product IS NOT NULL AND birth_date IS NOT NULL THEN 1 ELSE 0 END dados_pessoais
    ,CASE WHEN show_product IS NOT NULL AND birth_date IS NOT NULL THEN 1 ELSE 0 END data_de_nascimento
    ,CASE WHEN marital_status IS NOT NULL THEN 1 ELSE 0 END estado_civil
    ,CASE WHEN gender IS NOT NULL THEN 1 ELSE 0 END genero
    ,CASE WHEN place_of_birth IS NOT NULL THEN 1 ELSE 0 END estado_de_nascimento1
    ,CASE WHEN birth_city IS NOT NULL THEN 1 ELSE 0 END estado_de_nascimento2
    ,CASE WHEN national_identity IS NOT NULL THEN 1 ELSE 0 END rg
    ,CASE WHEN issuer_national_identity IS NOT NULL THEN 1 ELSE 0 END emissor_rg
    ,CASE WHEN issue_state_national_identity IS NOT NULL THEN 1 ELSE 0 END uf_rg
    ,CASE WHEN issuer_national_identity IS NOT NULL THEN 1 ELSE 0 END data_emissao_rg
    ,CASE WHEN address_zip_code IS NOT NULL THEN 1 ELSE 0 END dados_endereco
    ,CASE WHEN address_zip_code IS NOT NULL THEN 1 ELSE 0 END cep
    ,CASE WHEN address_street IS NOT NULL THEN 1 ELSE 0 END logradouro
    ,CASE WHEN address_number IS NOT NULL THEN 1 ELSE 0 END nro_endereco
    ,CASE WHEN address_neighborhood IS NOT NULL THEN 1 ELSE 0 END bairro
    ,CASE WHEN address_city IS NOT NULL THEN 1 ELSE 0 END cidade
    ,CASE WHEN address_state IS NOT NULL THEN 1 ELSE 0 END estado
    ,CASE WHEN bank IS NOT NULL THEN 1 ELSE 0 END dados_bancarios
    ,CASE WHEN bank IS NOT NULL THEN 1 ELSE 0 END banco
    ,CASE WHEN bank_agency IS NOT NULL THEN 1 ELSE 0 END nro_agencia
    ,CASE WHEN bank_account IS NOT NULL THEN 1 ELSE 0 END conta_corrente
    ,CASE WHEN bank_confirmation IS NOT NULL THEN 1 ELSE 0 END confirma_dados_bancarios
    ,CASE WHEN transfer_human IS NOT NULL THEN 1 ELSE 0 END transferencia_consultor_geral

FROM cte_context c
    join {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_from') }} f on f.message_session_id = c.message_session_id
)
select
    *
from cte_calculated
