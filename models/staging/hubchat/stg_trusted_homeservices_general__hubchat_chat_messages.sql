with 

source as (

    select * from {{ source('trusted_homeservices_general', 'hubchat_chat_messages') }}

),

renamed as (

    select
        __v,
        _id,
        data,
        lake_created_at,
        options,
        response,
        response_title,
        response_type,
        session_init,
        status,
        template_name,
        text,
        timestamp,
        token,
        who,
        year,
        month,
        day,
        lake_key,
        wpp_body,
        user_id

    from source

)

select 
    session_init message_session_id
    ,token
    ,user_id
    ,timestamp tsp_message
    ,who desc_message_source
    ,status desc_message_status
    ,text desc_message_text
    ,response_type 
    ,options
    ,wpp_body
    ,response
 from renamed