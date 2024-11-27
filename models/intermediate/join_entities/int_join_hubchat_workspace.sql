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
                        'Qwzep4lP1XuennMX','KPXwqncAzLJwng9R','iipRh6NiszHdSNsX','4yoRCujZlY0KCFRV',
                        'tlp034CFLTggvdAZ','kjSm43lPDG29jD0D') THEN 'HOMESERVICES'
        when token in ('quLecGCkR7Md6Xum') THEN 'TESTE'
    END AS vertical,
    CASE
        WHEN token IN ('ngFvs23MiWem4jNi', 'Z7Dng45tpO41Zglw', 'egFPMxIN01mLcA3j', 
                    '6pLRW38NZeb2X4cp', 'nzZfNjWOoZRiO7RR', 'VuShVz4wngVmQvdo', 'ex0D9QnnWW3oLgmw', 
                    'Ky19Pz6dWUywP5ZM', 'qsIfUT21CWeAYS3I','Wr6wQcKellisGwSk') THEN 'BV'
        WHEN token in ('QhDLpaXyJw6Ld79j','rtw7TGHCiXfweH0q') THEN 'QISTA'
        WHEN token IN ('fFfEZyM6bGvVr4vq', 'C3Dk8S2EbKCmtdqB', 'ITe3BFuPMWUvh2Sd', 
                        'mJhH9aUL15gN4t4Y', 'MUa6hKcWL7WgJY6T', 'GObxGIG8arJIaWHI') THEN 'ITAU'
        WHEN token IN ('5ZQmrzqIg6qCHtve','tG4xcFPdTPRF8dLQ','egLLr0RNOFH4vA2w','Qwzep4lP1XuennMX') THEN 'REENERGISA'
        WHEN token IN ( 'ScDVZX8g8Q3PbiPI', 'mSfULe6UnkxYBiQ3','MsDrMiwF4nhQH3Zm',
                        'PWWyzDv7PdpOxi4I','Q9Sosu4gqCZ6QWjH','4yoRCujZlY0KCFRV','tlp034CFLTggvdAZ','d9dyQCuPQh7LD5qZ','kjSm43lPDG29jD0D') THEN 'OI'
        WHEN token iN ('ePNrmsydThDAK5ij','4ebm9jE6Mtd0YSAR','CMXLEp8vqYcFMukB'
                        ,'gahKeIbyKUCyo58Z', 'c6suHCuPQh7LD3qZ','sy192VUvwtdm3AIe','KPXwqncAzLJwng9R','iipRh6NiszHdSNsX') THEN 'CLARO'
        WHEN token iN ('rZu49vt9xCmGbHPK','RJR2Wt8rHXdJ2hD3') THEN 'RODOBENS'
        when token IN ('quLecGCkR7Md6Xum') THEN 'TESTE'
    END AS brand,
    CASE
        WHEN token IN ('ngFvs23MiWem4jNi') THEN 'FINANCIAMENTO AUTO'
        WHEN token IN ('Z7Dng45tpO41Zglw','QhDLpaXyJw6Ld79j', 'fFfEZyM6bGvVr4vq', 'Ky19Pz6dWUywP5ZM',
        'qsIfUT21CWeAYS3I', 'GObxGIG8arJIaWHI')  THEN 'FGTS'
        WHEN token IN ('C3Dk8S2EbKCmtdqB',  'mJhH9aUL15gN4t4Y') THEN 'CONSIGNADO'
        WHEN token = 'egFPMxIN01mLcA3j' THEN 'CONTA CARTAO'
        WHEN token IN ('ITe3BFuPMWUvh2Sd','ex0D9QnnWW3oLgmw') THEN 'FINANCIAMENTO'
        WHEN token = 'MUa6hKcWL7WgJY6T' THEN 'CONSORCIO'
        WHEN token IN ('nzZfNjWOoZRiO7RR', '6pLRW38NZeb2X4cp', 'VuShVz4wngVmQvdo','Wr6wQcKellisGwSk') THEN 'EGV'
        WHEN token IN ('5ZQmrzqIg6qCHtve','Qwzep4lP1XuennMX') THEN 'ASSINATURA ENERGIA'
        WHEN token IN ('ePNrmsydThDAK5ij','4ebm9jE6Mtd0YSAR','CMXLEp8vqYcFMukB','d9dyQCuPQh7LD5qZ'
                        ,'gahKeIbyKUCyo58Z','c6suHCuPQh7LD3qZ','sy192VUvwtdm3AIe','KPXwqncAzLJwng9R'
                        ,'ScDVZX8g8Q3PbiPI', 'mSfULe6UnkxYBiQ3'
                        ,'PWWyzDv7PdpOxi4I','iipRh6NiszHdSNsX') THEN 'INTERNET, TV, TELEFONE E MOVEL'
        WHEN token IN ('tlp034CFLTggvdAZ','Q9Sosu4gqCZ6QWjH','d9dyQCuPQh7LD5qZ'
                       '4yoRCujZlY0KCFRV','MsDrMiwF4nhQH3Zm','kjSm43lPDG29jD0D') THEN 'INTERNET'
        when token IN ('quLecGCkR7Md6Xum') THEN 'TESTE'
    END AS product,
    CASE
        when token in ('MUa6hKcWL7WgJY6T') then 'ITAU CONSORCIO'
        when token in ('ITe3BFuPMWUvh2Sd') then 'ITAU FINANCIAMENTO'
        WHEN token in ('C3Dk8S2EbKCmtdqB') then 'ITAU CONSIGNADO'
        WHEN token in ('ex0D9QnnWW3oLgmw') then 'BV FINANCIAMENTO'
        WHEN token in ('Ky19Pz6dWUywP5ZM') then 'BV FGTS HOMOL'
        WHEN token in ('ngFvs23MiWem4jNi') then 'BV FINANCIAMENTO AUTO'
        WHEN token in ('Z7Dng45tpO41Zglw') then 'BV FGTS'
        WHEN token in ('egFPMxIN01mLcA3j') then 'BV CONTA E CARTAO'
        when token in ('quLecGCkR7Md6Xum') then 'ASSISTENTE DE TESTE - FINANCE'
        WHEN token in ('qsIfUT21CWeAYS3I') then '[HOM] Assistente BV FGTS'
        when token in ('QhDLpaXyJw6Ld79j') then 'QISTA FGTS'
        when token in ('Wr6wQcKellisGwSk') then 'BV EGV - FULL DIGITAL'
        WHEN token in ('fFfEZyM6bGvVr4vq') then 'ITAU FGTS'
        when token in ('5ZQmrzqIg6qCHtve') then 'REENERGISA 0800-350-4545'

        when token in ('4ebm9jE6Mtd0YSAR') then 'Claro - EVA'
        when token in ('4yoRCujZlY0KCFRV') then 'Oi Dealers 0800-244-4141'
        when token in ('PWWyzDv7PdpOxi4I') then 'Oi Dealers 0800-222-4141'
        when token in ('Q9Sosu4gqCZ6QWjH') then 'Oi Dealers 0800-288-4141'
        when token in ('Qwzep4lP1XuennMX') then 'Reenergisa 0800-722-0765'
        when token in ('ScDVZX8g8Q3PbiPI') then 'Oi Dealers 0800-250-4141'
        when token in ('ePNrmsydThDAK5ij') then 'Claro Afiliados (11) 97817-5214'
        when token in ('gahKeIbyKUCyo58Z') then 'Claro Atendimento (11) 97645-8661'
        when token in ('iipRh6NiszHdSNsX') then 'Claro Dealers 0800-123-2121'
        when token in ('kjSm43lPDG29jD0D') then 'POC voice OI'
        when token in ('sy192VUvwtdm3AIe') then 'Claro Dealers (11) 98891-3555'
        when token in ('tlp034CFLTggvdAZ') then 'Portal de Planos (11) 98891-5022'
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