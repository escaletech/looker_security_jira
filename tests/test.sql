-- tests/test_tabela_uniqueness.sql

SELECT 
    id
FROM 
    {{ ref("stg_raw__raw_finance_general_hubchat_escale_finance__attendance_test_dbt_core") }}  -- Referencia a tabela 'tabela' no modelo 'trusted'
WHERE ID = 1
GROUP BY 
    id

