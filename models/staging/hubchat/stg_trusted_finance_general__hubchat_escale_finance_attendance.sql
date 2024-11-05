with 

source as (

    select * from {{ source('trusted_finance_general', 'hubchat_escale_finance_attendance') }}

),

renamed as (

    select
        __v,
        _id,
        client_id,
        department_id,
        department_name,
        department_var_link,
        first_attendant_resp,
        lake_created_at,
        last_message,
        last_who,
        origin,
        robot_id,
        session_init,
        status,
        tabulation,
        tabulation_id,
        tabulation_internal_name,
        tabulation_name,
        tabulation_timestamp,
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
        wpp_timestamp_session,
        year,
        month,
        day,
        lake_key,
        rank() over(partition by session_init order by timestamp desc) as last_session

    from source
    QUALIFY last_session = 1

)

select 
    session_init
    ,user_id
    ,client_id
    ,tabulation_internal_name
    ,user_email
    ,user_fullname
from renamed
