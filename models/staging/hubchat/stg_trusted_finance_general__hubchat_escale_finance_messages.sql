with 

source as (

    select * from {{ source('trusted_finance_general', 'hubchat_escale_finance_messages') }}

),

renamed as (

    select
        __v,
        _id,
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
        who,
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
            WHEN options IS NOT NULL THEN options
            WHEN wpp_body IS NOT NULL THEN wpp_body
            WHEN response IS NOT NULL THEN response
            WHEN desc_message_text IS NOT NULL THEN desc_message_text 
        ELSE NULL 
    END AS group_text

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
    ,response_response_type as response_type
    ,options
    ,wpp_body
    ,response
    ,case when min(tsp_message) over(partition by message_session_id order by tsp_message) = tsp_message then 1 else 0 end as flag_first_msg
    ,case when max(tsp_message) over(partition by message_session_id order by tsp_message desc) = tsp_message then 1 else 0 end as flag_last_msg
    ,case when response_type = 'cron' then 1 else 0 end flag_timeout
    ,CASE 
        WHEN CHARINDEX('**', group_text) > 0 AND CHARINDEX('**', group_text, CHARINDEX('**', group_text) + 2) > CHARINDEX('**', group_text)
            THEN SUBSTRING(group_text, CHARINDEX('**', group_text) + 2, CHARINDEX('**', group_text, CHARINDEX('**', group_text) + 2) - CHARINDEX('**', group_text) - 2)
        WHEN CHARINDEX('*', group_text) > 0 AND CHARINDEX('*', group_text, CHARINDEX('*', group_text) + 1) > CHARINDEX('*', group_text)
            THEN SUBSTRING(group_text, CHARINDEX('*', group_text) + 1, CHARINDEX('*', group_text, CHARINDEX('*', group_text) + 1) - CHARINDEX('*', group_text) - 1)
        ELSE NULL 
        END AS code_product
from renamed
