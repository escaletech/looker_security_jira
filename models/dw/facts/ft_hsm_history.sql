with cte_int as (
select 
    message_session_id
    ,date_format(tsp_send_hsm,'yyyyMMdd') data_id
    ,vertical_id
    ,marca_id
    ,produto_id
    ,flowstep_id
    ,hsm
    ,tsp_send_hsm
from {{ ref('int_hubchat_hsm_history') }}
)
select * from cte_int