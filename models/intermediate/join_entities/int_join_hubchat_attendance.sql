with cte_join as (
    select * from {{ ref('stg_trusted_homeservices_general__hubchat_chat_attendance') }}
    union all
    select * from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_attendance') }}
)
, cte_unique_user as (
    select 
        user_email
        ,user_id
        ,timestamp
        ,rank() over(partition by user_email order by timestamp desc) as last_session
    from cte_join
    where user_id is not null
    and user_email is not null
    QUALIFY last_session = 1
)
, cte_backfill as (
select 
    j.* except(user_id)
    ,coalesce(j.user_id,uu.user_id) user_id
from cte_join j
left join cte_unique_user as uu on uu.user_email = j.user_email
)
select 
    hubchat_chat_messages_id
    ,user_id as atendente_id
    ,user_email
from cte_backfill