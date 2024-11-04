with cte_import_table as (
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
, cte_sum as (            
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
      FROM cte_import_table
)
, cte_lead_range as (
      SELECT 
            *,
            LEAD(faixa_acumulada, -1) OVER (ORDER BY ordem_faixa) faixa_acumulada_ant
      FROM cte_sum
)
, cte_join as(
      SELECT DISTINCT
            DATE_TRUNC('MONTH', tsp_message)::date AS dt_cost, 
            COUNT(CASE WHEN desc_message_source = 'user' OR response_type = 'cron' THEN hubchat_chat_messages_id ELSE NULL END) OVER (PARTITION BY to_date(tsp_message, 'yyyy-mm-01')) AS qnt_mgs
      FROM {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages') }}
      UNION ALL
      SELECT DISTINCT
            DATE_TRUNC('MONTH', tsp_message)::date AS dt_cost, 
            COUNT(CASE WHEN desc_message_source = 'user' OR response_type = 'cron' THEN hubchat_chat_messages_id ELSE NULL END) OVER (PARTITION BY to_date(tsp_message, 'yyyy-mm-01')) AS qnt_mgs
      FROM {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages') }}
)
, cte_group_by_month as (
      SELECT
            dt_cost, 
            sum(qnt_mgs) AS qnt_mgs
      FROM cte_join
      group by 1
)
SELECT
      mgs.dt_cost,
      mgs.qnt_mgs,
      CASE 
          WHEN mgs.dt_cost = DATE_TRUNC('MONTH', NOW()) 
          THEN LEAD(ROUND((((mgs.qnt_mgs-hp.faixa_ant)*hp.valor)+hp.faixa_acumulada_ant)/mgs.qnt_mgs, 7), -1) OVER(ORDER BY mgs.dt_cost ASC)
          ELSE ROUND((((mgs.qnt_mgs-hp.faixa_ant)*hp.valor)+hp.faixa_acumulada_ant)/mgs.qnt_mgs, 7) 
      END valor_mgs
FROM cte_group_by_month mgs
LEFT JOIN cte_lead_range hp ON  mgs.qnt_mgs BETWEEN hp.faixa_ini AND hp.faixa_fim AND dt_cost BETWEEN hp.data_ini AND IFNULL(data_fim, current_date)
