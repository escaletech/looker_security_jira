with cte_join_tables as (
    select 
        mgs.* 
        ,coalesce(HC.valor_mgs,0) as valor_mgs
    from {{ ref('int_join_hubchat_messages') }} mgs
    left join {{ ref('int_hubspot_cost') }} HC ON  HC.dt_cost = DATE_TRUNC('MONTH',mgs.tsp_message) and (mgs.desc_message_source = "user" OR mgs.response_type = "cron")
)
,cte_metrics as (
    select 
        *
    from cte_join_tables
)
select * from cte_metrics
