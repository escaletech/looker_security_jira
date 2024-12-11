with cte_valid_types as(
        select 
            a.source 
            ,a.attendance_id
        from {{ ref('int_join_phonemanager_attendence') }} a
    left join {{ ref('int_join_phonemanager_attendence_status_type') }}  st on st.status_id = a.status_id and st.source = a.source
    --left join {{ ref('int_join_phonemanager_attendence_status_type') }}  st2 on st2.status_id = a.type_id and st2.source = a.source
    where true
        and st.desc_status_attendance not in ('CANCELADA','VENDA DUPLICADA','TESTE VENDA','VENDA TESTE','DUPLICIDADE DE PEDIDO','DUPLICIDADE DE CPF')
        and a.type_id = 1 -- ajustar para a venda
)
,cte_phonemanager_group as (
    select distinct
        ap.source
        , ap.attendance_id
        , 1 qtde_deals_aprovado  
    from {{ ref('int_join_phonemanager_attendence_products') }} ap
    join cte_valid_types on cte_valid_types.attendance_id = ap.attendance_id and cte_valid_types.source = ap.source
    where group_attribute = 'finalizacao' and value_name <> ''
)
, cte_join_products_source as (
    select 
        message_session_id
        ,1 qtde_deals_total
        ,coalesce(qtde_deals_aprovado,0) qtde_deals_aprovado
        ,case when qtde_deals_aprovado = 1 then 0 else 1 end qtde_deals_declinado
    from {{ ref('int_join_phonemanager_attendence_source') }} s
    left join cte_phonemanager_group pg on pg.attendance_id = s.attendance_id and pg.source = s.source
    where message_session_id is not null
)
select 
    *
from cte_join_products_source