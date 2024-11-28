with cte_join as (
    select 'a' base ,message_session_id, count(*) qtde_deals_total, count(dt_venda) qtde_deals_aprovado, count(dt_declinado) qtde_deals_declinado from {{ ref('stg_trusted_finance_bv__hubspot_deals') }}
    where message_session_id is not null
    group by all
    union all
    select 'b' base ,message_session_id, count(*) qtde_deals_total, count(dt_venda) qtde_deals_aprovado, count(dt_declinado) qtde_deals_declinado from {{ ref('stg_trusted_finance_itau__hubspot_deals') }}
    where message_session_id is not null
    group by all
    union all
    select 'c' base ,message_session_id, sum(qtde_deals_total) qtde_deals_total, sum(qtde_deals_aprovado) qtde_deals_aprovado, sum(qtde_deals_declinado) qtde_deals_declinado from {{ ref('int_phonemanager_valid_deal') }}
    group by all
)
select 
    *
from cte_join