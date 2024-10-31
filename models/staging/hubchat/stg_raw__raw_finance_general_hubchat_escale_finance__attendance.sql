with 

source as (

    select * from {{ source('raw', 'raw_finance_general_hubchat_escale_finance__attendance') }}

),

renamed as (

    select
        __v,
        _id,
        department_id,
        department_name,
        department_var_link,
        TRY_CAST(first_attendant_resp AS timestamp) as first_attendant_resp,
        lake_created_at,
        origin,
        session_init,
        tabulation,
        --TRY_CAST(tabulation_timestamp AS timestamp) AS tabulation_timestamp,
        TRY_CAST(timestamp AS timestamp) AS timestamp,
        TRY_CAST(timestamp_init AS timestamp) timestamp_init,
        token,
        user_email,
        user_fullname,
        user_id,
        year,
        month,
        day

    from source
)

select 
    session_init
    ,user_id
    ,token
    ,timestamp
    ,timestamp_init
    ,first_attendant_resp
    ,user_fullname
    ,user_email
    ,department_name
    ,tabulation
from renamed
order by 1