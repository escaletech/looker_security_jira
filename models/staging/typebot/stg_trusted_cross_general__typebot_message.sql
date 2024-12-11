with 

source as (

    select * from {{ source('trusted_cross_general', 'typebot_message') }}

),

renamed as (

    select
        _id,
        created_at,
        lake_created_at,
        message_id,
        metadata_from_me,
        metadata_instance_id,
        metadata_key_from_me,
        metadata_key_id,
        metadata_key_remote_jid,
        metadata_message_id,
        metadata_message_timestamp,
        metadata_message_type,
        metadata_message_conversation,
        metadata_message_image_message_file_name,
        metadata_message_image_message_gif_playback,
        metadata_message_image_message_id,
        metadata_message_image_message_media,
        metadata_message_image_message_media_type,
        metadata_message_image_message_mimetype,
        metadata_message_image_message_type,
        metadata_participant,
        metadata_push_name,
        metadata_remote_jid,
        metadata_session_await_user,
        metadata_session_bot_id,
        metadata_session_context,
        metadata_session_created_at,
        metadata_session_id,
        metadata_session_instance_id,
        metadata_session_parameters_api_key,
        metadata_session_parameters_instance_name,
        metadata_session_parameters_owner_jid,
        metadata_session_parameters_push_name,
        metadata_session_parameters_remote_jid,
        metadata_session_parameters_server_url,
        metadata_session_push_name,
        metadata_session_remote_jid,
        metadata_session_session_id,
        metadata_session_status,
        metadata_session_type,
        metadata_session_updated_at,
        metadata_source,
        metadata_status,
        remote_jid,
        session_id,
        status,
        text,
        updated_at,
        who,
        who_id,
        year,
        month,
        day,
        lake_key

    from source

)

select 
    message_id
    ,session_id
    ,metadata_session_bot_id
    ,metadata_participant
    ,metadata_session_created_at
    ,metadata_session_parameters_instance_name hsm
    ,status
    ,text desc_message_text
    ,who desc_message_source
 from renamed


  /*  ,token
    ,user_id
    ,timestamp tsp_message
    ,response_response_type as response_type
    ,text desc_message_text
    ,case when response_type = 'cron' then 1 else 0 end flag_timeout
    */