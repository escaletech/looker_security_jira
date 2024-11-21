with cte_join as(
    select 'pdp' source, * from {{ ref('stg_trusted_homeservices_pdp__phonemanager_attendances_products') }}
    union all
    select 'claro' source, * from {{ ref('stg_trusted_homeservices_claro__phonemanager_attendances_products') }}
)
select * from cte_join 