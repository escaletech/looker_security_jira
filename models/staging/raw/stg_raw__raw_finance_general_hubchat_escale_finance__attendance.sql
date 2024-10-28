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
        first_attendant_resp,
        lake_created_at,
        origin,
        session_init,
        tabulation,
        tabulation_timestamp,
        timestamp,
        timestamp_init,
        token,
        user_email,
        user_fullname,
        user_id,
        year,
        month,
        day

    from source

)

select * from renamed
