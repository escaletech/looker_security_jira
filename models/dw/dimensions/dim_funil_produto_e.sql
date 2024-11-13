--01. [ITAÚ - FGTS] Funil Conversacional
with cte_context as (
    SELECT
        c.*         
    FROM {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_context') }} c
    left join {{ ref('int_join_hubchat_workspace') }} w on w.token = c.token
    left join {{ ref('dim_flowstep') }} f on f.flowstep_id = w.flowstep_id
    where true
        --and welcome = 'true'
        and desc_flowstep = 'FINANCE - ITAU - FGTS'
)
, cte_calculated as (
SELECT
    c.message_session_id
    ,c.timestamp
    ,CASE WHEN c.welcome = 'true' THEN 1 ELSE 0 END AS welcome
    ,CASE WHEN c.individual_registration = 'true' THEN 1 ELSE 0 END AS identificacao_cpf
    ,CASE WHEN c.name = 'true' THEN 1 ELSE 0 END AS nome
    ,CASE WHEN c.eligible_product = 'true' THEN 1 ELSE 0 END AS simulacao_elegibilidade
    ,CASE WHEN c.email IS NOT NULL THEN 1 ELSE 0 END AS dados_pessoais_email
    ,CASE WHEN c.gender IS NOT NULL THEN 1 ELSE 0 END AS genero
    ,CASE WHEN c.mother_name IS NOT NULL THEN 1 ELSE 0 END AS nome_da_mae
    ,CASE WHEN c.marital_status IS NOT NULL THEN 1 ELSE 0 END AS estado_civil
    ,CASE WHEN c.nationality IS NOT NULL THEN 1 ELSE 0 END AS validacao_do_documento
    ,CASE WHEN c.profession IS NOT NULL THEN 1 ELSE 0 END AS profissao
    ,CASE WHEN c.income_value IS NOT NULL THEN 1 ELSE 0 END AS renda
    ,CASE WHEN c.address_zip_code IS NOT NULL THEN 1 ELSE 0 END AS endereco_cep
    ,CASE WHEN c.address_number IS NOT NULL THEN 1 ELSE 0 END AS nro_end
    ,CASE WHEN c.bank IS NOT NULL THEN 1 ELSE 0 END AS dados_bancarios_banco
    ,CASE WHEN c.bank_agency IS NOT NULL THEN 1 ELSE 0 END AS agencia
    ,CASE WHEN c.bank_agency IS NOT NULL THEN 1 ELSE 0 END AS nro_da_conta

FROM cte_context c
    join {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_from') }} f on f.message_session_id = c.message_session_id
    join {{ ref('stg_trusted_finance_general__hubchat_escale_finance_attendance') }} a on a.message_session_id = c.message_session_id
)
select
    *
from cte_calculated c