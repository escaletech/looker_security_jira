with cte_phonemanager_group as (
    select 
        source
        , attendance_id
        , max(case when group_attribute = 'data_habilitacao' then 1 else 0 end ) flag_habilitado  
    from {{ ref('int_join_phonemanager_attendence_products') }}
    group by all
)
, cte_join_products_source as (
    select
        message_session_id
        ,sum(flag_habilitado) qtde_deals
        ,count(*)-sum(flag_habilitado) qtde_deals_declinado
    from {{ ref('int_join_phonemanager_attendence_source') }} s
    left join cte_phonemanager_group pg on pg.attendance_id = s.attendance_id and pg.source = s.source
    where message_session_id is not null
    group by 1
)
,cte_join as (
    select message_session_id, count(dt_venda) qtde_deals, count(*)-count(dt_venda) qtde_deals_declinado from {{ ref('stg_trusted_finance_bv__hubspot_deals') }}
    where message_session_id is not null
    group by 1
    union all
    select message_session_id, count(dt_venda) qtde_deals, count(*)-count(dt_venda) qtde_deals_declinado from {{ ref('stg_trusted_finance_itau__hubspot_deals') }}
    where message_session_id is not null
    group by 1
    union all
    select * from cte_join_products_source
)
select 
    * 
from cte_join