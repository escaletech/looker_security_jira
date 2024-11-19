with cte_join as (
    select message_session_id, count(dt_venda) qtde_deals, count(*)-count(dt_venda) qtde_deals_declinado from {{ ref('stg_trusted_finance_bv__hubspot_deals') }}
    where message_session_id is not null
    group by 1
    union all
    select message_session_id, count(dt_venda) qtde_deals, count(*)-count(dt_venda) qtde_deals_declinado from {{ ref('stg_trusted_finance_itau__hubspot_deals') }}
    where message_session_id is not null
    group by 1
    --union all
    --select distinct message_session_id from {{ ref('stg_trusted_homeservices_pdp__phonemanager_attendances_source') }}
    --union all
    --select distinct message_session_id from {{ ref('stg_trusted_homeservices_claro__phonemanager_attendances_source') }}
)
select 
    * 
from cte_join