with 

source as (

    select * from {{ source('raw', 'raw_finance_general_hubchat_escale_finance__messages_from') }}

),

renamed as (

    select
        __v,
        _id,
        lake_created_at,
        origin,
        session_init,
        timestamp,
        timestamp_init,
        token,
        user_agent,
        year,
        month,
        day

    from source

)

select * from renamed
