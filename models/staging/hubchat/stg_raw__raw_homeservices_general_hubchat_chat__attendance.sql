with 

source as (

    select * from {{ source('raw', 'raw_homeservices_general_hubchat_chat__attendance') }}

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
        --tabulation_timestamp,
        timestamp,
        timestamp_init,
        token,
        user_email,
        user_fullname,
        user_id,
        month,
        day

    from source

)

select distinct department_id from renamed
