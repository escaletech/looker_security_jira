 
select distinct 
    atendente_id
    ,user_email
from {{ ref('int_join_hubchat_attendance') }}
union all
select
    -1
    ,'SEM ATENDENTE'