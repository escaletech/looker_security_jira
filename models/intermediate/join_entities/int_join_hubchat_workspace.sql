with cte_join as(
    select * from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_workspace') }}
    union all
    select * from {{ ref('stg_trusted_homeservices_general__hubchat_chat_workspace') }}
)
,cte_token_identification as (
select 
    CASE
    WHEN token IN ('ngFvs23MiWem4jNi', 'Z7Dng45tpO41Zglw', 'egFPMxIN01mLcA3j', 
                '6pLRW38NZeb2X4cp', 'nzZfNjWOoZRiO7RR', 'VuShVz4wngVmQvdo', 
                'QhDLpaXyJw6Ld79j', 'fFfEZyM6bGvVr4vq', 'C3Dk8S2EbKCmtdqB', 
                'ITe3BFuPMWUvh2Sd', 'ex0D9QnnWW3oLgmw', 'Ky19Pz6dWUywP5ZM', 
                'qsIfUT21CWeAYS3I', 'mJhH9aUL15gN4t4Y', 'MUa6hKcWL7WgJY6T', 'GObxGIG8arJIaWHI') THEN 'FINANCE'
    WHEN token IN ('5ZQmrzqIg6qCHtve', 'ScDVZX8g8Q3PbiPI', 'mSfULe6UnkxYBiQ3', 
                'ePNrmsydThDAK5ij') THEN 'HOMESERVICES'
    END AS vertical,
    CASE
    WHEN token IN ('ngFvs23MiWem4jNi', 'Z7Dng45tpO41Zglw', 'egFPMxIN01mLcA3j', 
                '6pLRW38NZeb2X4cp', 'nzZfNjWOoZRiO7RR', 'VuShVz4wngVmQvdo', 'ex0D9QnnWW3oLgmw', 
                'Ky19Pz6dWUywP5ZM', 'qsIfUT21CWeAYS3I') THEN 'BV'
    WHEN token = 'QhDLpaXyJw6Ld79j' THEN 'QISTA'
    WHEN token IN ('fFfEZyM6bGvVr4vq', 'C3Dk8S2EbKCmtdqB', 'ITe3BFuPMWUvh2Sd', 
    'mJhH9aUL15gN4t4Y', 'MUa6hKcWL7WgJY6T', 'GObxGIG8arJIaWHI') THEN 'ITAU'
    WHEN token IN ('5ZQmrzqIg6qCHtve') THEN 'REENERGISA'
    WHEN token IN ('ScDVZX8g8Q3PbiPI', 'mSfULe6UnkxYBiQ3') THEN 'OI'
    WHEN token iN ('ePNrmsydThDAK5ij') THEN 'CLARO'
    END AS brand,
    
    CASE
    WHEN token IN ('ngFvs23MiWem4jNi') THEN 'FINANCIAMENTO AUTO'
    WHEN token IN ('Z7Dng45tpO41Zglw','QhDLpaXyJw6Ld79j', 'fFfEZyM6bGvVr4vq', 'Ky19Pz6dWUywP5ZM',
    'qsIfUT21CWeAYS3I', 'GObxGIG8arJIaWHI')  THEN 'FGTS'
    WHEN token IN ('C3Dk8S2EbKCmtdqB',  'mJhH9aUL15gN4t4Y') THEN 'CONSIGNADO'
    WHEN token = 'egFPMxIN01mLcA3j' THEN 'CONTA CARTAO'
    WHEN token = 'ITe3BFuPMWUvh2Sd' THEN 'FINANCIAMENTO'
    WHEN token = 'MUa6hKcWL7WgJY6T' THEN 'CONSORCIO'
    WHEN token IN ('nzZfNjWOoZRiO7RR', '6pLRW38NZeb2X4cp', 'VuShVz4wngVmQvdo') THEN 'EGV'
    WHEN token IN ('5ZQmrzqIg6qCHtve') THEN 'ASSINATURA ENERGIA'
    WHEN token IN ('ScDVZX8g8Q3PbiPI', 'mSfULe6UnkxYBiQ3', 'ePNrmsydThDAK5ij') THEN 'NAO INFORMADO'
    END AS product
    CASE
        WHEN token in ('C3Dk8S2EbKCmtdqB') then 'FINANCE - CONSIGNADO'
        ELSE 'NAO INFORMADO'
        end funil
    ,token
    ,assistant_name
from cte_join
)
, CTE_FILL_TABLE AS (
select 
    coalesce(vertical,'NAO INFORMADO') AS vertical
    ,coalesce(brand,'NAO INFORMADO') AS brand
    ,coalesce(product,'NAO INFORMADO') AS product
    ,token
    ,assistant_name
FROM cte_token_identification
)
SELECT 
    token
    ,dv.vertical_id
    ,dm.marca_id
    ,dp.produto_id    
FROM CTE_FILL_TABLE cft
left join {{ ref('dim_vertical') }} dv on dv.vertical = cft.vertical
left join {{ ref('dim_marca') }} dm on dm.marca = cft.brand
left join {{ ref('dim_produto') }} dp on dp.produto = cft.product