with 

source as (

    select * from {{ source('trusted_finance_general', 'hubchat_escale_finance_messages') }}

)

,renamed as (

    select
        __v,
        _id as hubchat_chat_messages_id,
        lake_created_at,
        message_statuses_new,
        options,
        response,
        response_response_type,
        response_source,
        session_init,
        text,
        timestamp,
        token,
        user_id,
        case when who in ('watson','hubchat') then 'hubchat' else who end who,
        wpp_message_id,
        year,
        month,
        day,
        lake_key,
        wpp_body,
        wpp_body_image_caption,
        data,
        wpp_body_audio_caption,
        wpp_body_audio_type,
        wpp_body_audio_url,
        wpp_body_image_type,
        wpp_body_image_url,
        message_statuses,
        message_status,
        CASE 
            WHEN text IS NOT NULL THEN text 
            WHEN options IS NOT NULL THEN options
            WHEN wpp_body IS NOT NULL THEN wpp_body
            WHEN response IS NOT NULL THEN response
        ELSE NULL 
        END AS group_text,
        case when (who = "user" OR who = "cron") then 1 else 0 end flag_paid_msg
        --lag(timestamp) over(partition by session_init order by timestamp) lag_tsp_message
    from source

)

select 
    hubchat_chat_messages_id
    , session_init message_session_id
    ,token
    ,user_id
    ,timestamp tsp_message
    ,who desc_message_source
    ,message_status desc_message_status
    ,response_response_type as response_type
    ,text desc_message_text
    ,options
    ,wpp_body
    ,response
    ,group_text
    ,min(timestamp) over(partition by session_init order by timestamp) as tsp_first_msg
    ,min(timestamp) over(partition by session_init, who = 'user' order by timestamp) as tsp_first_user_msg
    ,min(timestamp) over(partition by session_init, user_id is not null order by timestamp) as tsp_first_agent_msg
    ,max(timestamp) over(partition by session_init, who = 'hubchat', user_id is null order by timestamp) as tsp_last_bot_msg
    ,max(timestamp) over(partition by session_init order by timestamp desc) as tsp_last_msg
    ,case when response_type = 'cron' then 1 else 0 end flag_timeout
    ,'' AS desc_digital_campaing
    ,flag_paid_msg
    ,'' as hsm_template
    --,timestampdiff(SECOND, lag_tsp_message, timestamp)/60 vlr_tempo_resposta
from renamed
