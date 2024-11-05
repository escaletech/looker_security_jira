with 

source as (

    select * from {{ source('trusted_homeservices_general', 'hubchat_chat_attendance') }}

),

renamed as (

    select
        __v,
        _id,
        department_id,
        department_name,
        department_var_link,
        first_attendant_resp,
        lake_created_at,
        origin,
        session_init,
        tabulation,
        tabulation_id,
        tabulation_internal_name,
        tabulation_name,
        tabulation_who_email,
        tabulation_who_id,
        tabulation_who_name,
        tabulation_who_type,
        timestamp,
        timestamp_init,
        token,
        user_email,
        user_fullname,
        user_id,
        year,
        month,
        day,
        lake_key,
        tabulation_timestamp,
        rank() over(partition by session_init order by timestamp desc) as last_session

    from source
    QUALIFY last_session = 1
)

select --session_init, count(*)
    session_init
    ,user_id
    ,'' as client_id
    ,tabulation_internal_name
    ,user_email
    ,user_fullname
from renamed