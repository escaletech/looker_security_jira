with 

source as (

    select * from {{ source('trusted_homeservices_general', 'hubchat_chat_messages') }}

)

--,renamed as (

    select
        __v,
        _id as hubchat_chat_messages_id,
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
        case when who = 'watson' then 'hubchat' else who end desc_message_source,
        year,
        month,
        day,
        lake_key,
        wpp_body,
        user_id,
        CASE 
            WHEN options IS NOT NULL THEN options
            WHEN wpp_body IS NOT NULL THEN wpp_body
            WHEN response IS NOT NULL THEN response
            WHEN text IS NOT NULL THEN text 
        ELSE NULL 
        END AS group_text,
        case when (who = 'user' OR who = 'cron') then 1 else 0 end flag_paid_msg
        --lag(timestamp) over(partition by session_init order by timestamp) lag_tsp_message

    from source

)

select
    hubchat_chat_messages_id
    ,session_init message_session_id
    ,token
    ,user_id
    ,timestamp tsp_message
    ,desc_message_source
    ,status desc_message_status
    ,text desc_message_text
    ,response_type 
    ,options
    ,wpp_body
    ,response
    ,min(timestamp) over(partition by session_init order by timestamp) as tsp_first_msg
    ,min(timestamp) over(partition by session_init, desc_message_source = 'user' order by timestamp) as tsp_first_user_msg
    ,min(timestamp) over(partition by session_init, user_id is not null order by timestamp) as tsp_first_agent_msg
    ,max(timestamp) over(partition by session_init, desc_message_source = 'hubchat', user_id is null order by timestamp) as tsp_last_bot_msg
    ,max(timestamp) over(partition by session_init order by timestamp desc) as tsp_last_msg
    ,case when response_type = 'cron' then 1 else 0 end flag_timeout
    ,CASE 
        WHEN CHARINDEX('**', group_text) > 0 AND CHARINDEX('**', group_text, CHARINDEX('**', group_text) + 2) > CHARINDEX('**', group_text)
            THEN SUBSTRING(group_text, CHARINDEX('**', group_text) + 2, CHARINDEX('**', group_text, CHARINDEX('**', group_text) + 2) - CHARINDEX('**', group_text) - 2)
        WHEN CHARINDEX('*', group_text) > 0 AND CHARINDEX('*', group_text, CHARINDEX('*', group_text) + 1) > CHARINDEX('*', group_text)
            THEN SUBSTRING(group_text, CHARINDEX('*', group_text) + 1, CHARINDEX('*', group_text, CHARINDEX('*', group_text) + 1) - CHARINDEX('*', group_text) - 1)
        ELSE NULL 
        END AS desc_digital_campaing
    ,flag_paid_msg
    ,template_name hsm_template
    --,timestampdiff(SECOND, lag_tsp_message, timestamp)/60 vlr_tempo_resposta
 from renamed