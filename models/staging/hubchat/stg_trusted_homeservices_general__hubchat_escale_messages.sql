with 

source as (

    select * from {{ source('trusted_homeservices_general', 'hubchat_escale_messages') }}

),

renamed as (

    select
        __v,
        _id,
        data,
        lake_created_at,
        session_init,
        text,
        timestamp,
        token,
        user_id,
        who,
        year,
        month,
        day,
        lake_key,
        message_status

    from source

)

select 
    session_init message_session_id
    ,token
    ,user_id
    ,timestamp tsp_message
    ,who desc_message_source
    ,message_status desc_message_status
    ,text desc_message_text
    ,'' as response_type
    ,'' as response_source
 from renamed
