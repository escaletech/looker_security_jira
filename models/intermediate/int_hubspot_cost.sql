with a as (
      SELECT 
            data_ini,
            data_fim,
            faixa_ini,
            faixa_fim,
            valor,
            LEAD(valor, -1) OVER (ORDER BY data_ini, data_fim, faixa_fim ASC) valor_ant,
            LEAD(faixa_fim, -1) OVER (ORDER BY data_ini, data_fim, faixa_fim ASC) faixa_ant,
            ROW_NUMBER() OVER (ORDER BY data_ini, data_fim, faixa_fim ASC) ordem_faixa,
            IFNULL(faixa_fim - LEAD(faixa_fim, -1) OVER (ORDER BY data_ini, data_fim, faixa_fim ASC), faixa_fim) * valor valor_faixa
      FROM {{ ref('stg_trusted_homeservices_general__google_spreadsheet_hubchat_price') }}
)
, b as (            
      SELECT
            data_ini,
            data_fim,
            faixa_ini,
            faixa_fim,
            valor,
            valor_ant,
            faixa_ant,
            valor_faixa,
            ordem_faixa,
            SUM(valor_faixa) OVER (ORDER BY ordem_faixa) faixa_acumulada
      FROM a
)
, c as (
      SELECT 
            *,
            LEAD(faixa_acumulada, -1) OVER (ORDER BY ordem_faixa) faixa_acumulada_ant
      FROM b
)
, d as(
      SELECT
            timestamp,
            who,
            response_type,
            _id
      FROM  refined_homeservices_general.general_hubchat_messages
      UNION ALL
      SELECT
            timestamp,
            who,
            response_response_type,
            _id
      FROM trusted_finance_general.hubchat_escale_finance_messages
)
, e as (
      SELECT DISTINCT
            DATE_TRUNC('MONTH', timestamp) AS timestamp, 
            COUNT(CASE WHEN who = 'user' OR response_type = 'cron' THEN _id ELSE NULL END) OVER (PARTITION BY DATE_TRUNC('MONTH', timestamp)) AS qnt_mgs
      FROM d
)
SELECT
      mgs.timestamp,
      mgs.qnt_mgs,
      CASE 
          WHEN mgs.timestamp = DATE_TRUNC('MONTH', NOW()) 
          THEN LEAD(ROUND((((mgs.qnt_mgs-hp.faixa_ant)*hp.valor)+hp.faixa_acumulada_ant)/mgs.qnt_mgs, 7), -1) OVER(ORDER BY mgs.timestamp ASC)
          ELSE ROUND((((mgs.qnt_mgs-hp.faixa_ant)*hp.valor)+hp.faixa_acumulada_ant)/mgs.qnt_mgs, 7) 
      END valor_mgs
FROM e mgs
LEFT JOIN c hp ON  mgs.qnt_mgs BETWEEN hp.faixa_ini AND hp.faixa_fim AND CAST(mgs.timestamp AS DATE) BETWEEN CAST(data_ini AS DATE) AND CAST(IFNULL(data_fim, NOW()) AS DATE)
