with cte_join as(
    select * from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_workspace') }}
    union all
    select * from {{ ref('stg_trusted_homeservices_general__hubchat_chat_workspace') }}
)
,cte_token_identification as (
select 
    CASE
        WHEN token IN ( 'ngFvs23MiWem4jNi', 'Z7Dng45tpO41Zglw', 'egFPMxIN01mLcA3j', 
                        '6pLRW38NZeb2X4cp', 'nzZfNjWOoZRiO7RR', 'VuShVz4wngVmQvdo', 
                        'QhDLpaXyJw6Ld79j', 'fFfEZyM6bGvVr4vq', 'C3Dk8S2EbKCmtdqB', 
                        'ITe3BFuPMWUvh2Sd', 'ex0D9QnnWW3oLgmw', 'Ky19Pz6dWUywP5ZM', 
                        'qsIfUT21CWeAYS3I', 'mJhH9aUL15gN4t4Y', 'MUa6hKcWL7WgJY6T',
                        'Wr6wQcKellisGwSk', 'GObxGIG8arJIaWHI', 'rtw7TGHCiXfweH0q',
                        'rZu49vt9xCmGbHPK', 'RJR2Wt8rHXdJ2hD3','Mxrh8nq6YggPHwBZ') THEN 'FINANCE'
        WHEN token IN ( '5ZQmrzqIg6qCHtve','ScDVZX8g8Q3PbiPI','mSfULe6UnkxYBiQ3', 
                        'ePNrmsydThDAK5ij','4ebm9jE6Mtd0YSAR','CMXLEp8vqYcFMukB','d9dyQCuPQh7LD5qZ',
                        'gahKeIbyKUCyo58Z','c6suHCuPQh7LD3qZ','sy192VUvwtdm3AIe','MsDrMiwF4nhQH3Zm',
                        'PWWyzDv7PdpOxi4I','Q9Sosu4gqCZ6QWjH','egLLr0RNOFH4vA2w','tG4xcFPdTPRF8dLQ',
                        'Qwzep4lP1XuennMX','KPXwqncAzLJwng9R','iipRh6NiszHdSNsX','4yoRCujZlY0KCFRV') THEN 'HOMESERVICES'
    END AS vertical,
    CASE
        WHEN token IN ('ngFvs23MiWem4jNi', 'Z7Dng45tpO41Zglw', 'egFPMxIN01mLcA3j', 
                    '6pLRW38NZeb2X4cp', 'nzZfNjWOoZRiO7RR', 'VuShVz4wngVmQvdo', 'ex0D9QnnWW3oLgmw', 
                    'Ky19Pz6dWUywP5ZM', 'qsIfUT21CWeAYS3I') THEN 'BV'
        WHEN token in ('Wr6wQcKellisGwSk','QhDLpaXyJw6Ld79j','rtw7TGHCiXfweH0q') THEN 'QISTA'
        WHEN token IN ('fFfEZyM6bGvVr4vq', 'C3Dk8S2EbKCmtdqB', 'ITe3BFuPMWUvh2Sd', 
                        'mJhH9aUL15gN4t4Y', 'MUa6hKcWL7WgJY6T', 'GObxGIG8arJIaWHI') THEN 'ITAU'
        WHEN token IN ('5ZQmrzqIg6qCHtve','tG4xcFPdTPRF8dLQ','egLLr0RNOFH4vA2w','Qwzep4lP1XuennMX') THEN 'REENERGISA'
        WHEN token IN ( 'ScDVZX8g8Q3PbiPI', 'mSfULe6UnkxYBiQ3','MsDrMiwF4nhQH3Zm',
                        'PWWyzDv7PdpOxi4I','Q9Sosu4gqCZ6QWjH') THEN 'OI'
        WHEN token iN ('ePNrmsydThDAK5ij','4ebm9jE6Mtd0YSAR','CMXLEp8vqYcFMukB','d9dyQCuPQh7LD5qZ'
                        ,'gahKeIbyKUCyo58Z', 'c6suHCuPQh7LD3qZ','sy192VUvwtdm3AIe','KPXwqncAzLJwng9R') THEN 'CLARO'
        WHEN token iN ('rZu49vt9xCmGbHPK','RJR2Wt8rHXdJ2hD3') THEN 'RODOBENS'
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
        END AS product,
    CASE
        WHEN token in ('C3Dk8S2EbKCmtdqB') then 'FINANCE - ITAU - CONSIGNADO'
        WHEN token in ('ngFvs23MiWem4jNi') then 'FINANCE - BV - INBOUND/OUTBOUND AUTO'
        WHEN token in ('ScDVZX8g8Q3PbiPI', 'mSfULe6UnkxYBiQ3') then 'HOMESERVICES- OI - VENDA'
        WHEN token in ('fFfEZyM6bGvVr4vq') then 'FINANCE - ITAU - FGTS'
        WHEN token in ('Mxrh8nq6YggPHwBZ','quLecGCkR7Md6Xum','C3Dk8S2EbKCmtdqB','ngFvs23MiWem4jNi') then 'FINANCE - BV - FINANCIAMENTO'
        ELSE 'NAO INFORMADO'
        end flowstep
    ,token
    ,assistant_name
from cte_join
)
, CTE_FILL_TABLE AS (
select 
    coalesce(vertical,'NAO INFORMADO') AS vertical
    ,coalesce(brand,'NAO INFORMADO') AS brand
    ,coalesce(product,'NAO INFORMADO') AS product
    ,coalesce(flowstep,'NAO INFORMADO') AS flowstep
    ,token
    ,assistant_name
FROM cte_token_identification
)
, cte_main as (
SELECT 
    token
    ,dv.vertical_id
    ,dm.marca_id
    ,dp.produto_id
    ,df.flowstep_id
    ,assistant_name
FROM CTE_FILL_TABLE cft
left join {{ ref('dim_vertical') }} dv on dv.vertical = cft.vertical
left join {{ ref('dim_marca') }} dm on dm.marca = cft.brand
left join {{ ref('dim_produto') }} dp on dp.produto = cft.product
left join {{ ref('dim_flowstep') }} df on df.desc_flowstep = cft.flowstep
ORDER BY 6
)
select * from cte_main
--token = 'QW4W85h2R969KnbA' não existe no workspace, porém existe na messages