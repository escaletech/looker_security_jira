with cte_join as (
    select * from {{ ref('stg_trusted_homeservices_general__hubchat_chat_attendance') }}
    union all
    select * from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_attendance') }}
)
select * from cte_join