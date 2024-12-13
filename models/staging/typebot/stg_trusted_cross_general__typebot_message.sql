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
        lake_key,
        CASE 
            WHEN TRY_PARSE_JSON(text) IS NULL THEN text
            ELSE null
        END AS body,
        CASE 
            WHEN TRY_PARSE_JSON(text) IS NOT NULL THEN CAST(PARSE_JSON(text) AS STRING) 
            ELSE null
        END AS json_body

    from source

)

select 
    message_id conversacional_id
    ,session_id message_session_id
    ,date_format(created_at,'yyyyMMdd') as data_id
    ,created_at as tsp_message
    ,rank() over(partition by session_id order by created_at) nr_order_msg
    ,metadata_session_parameters_instance_name bot_agent
    ,who desc_message_source
    ,coalesce(coalesce(body,get_json_object(json_body, '$.body.text')),json_body) desc_message_text
    ,json_body
    , case when status in ('CREATED','SENT','DELIVERED','READ') then 1 else 0 end flag_criado
    , case when status in ('SENT','DELIVERED','READ') then 1 else 0 end flag_enviado
    , case when status in ('DELIVERED','READ') then 1 else 0 end flag_entregue
    , case when status = 'READ' then 1 else 0 end flag_lido
    , case 
        when get_json_object(json_body, '$.action.parameters.flow_id') is not null then get_json_object(json_body, '$.action.parameters.flow_id')
        when get_json_object(json_body, '$.wa_flow_response_params.flow_id') is not null then get_json_object(json_body, '$.wa_flow_response_params.flow_id')
        end AS flow_id
    , case 
        when get_json_object(json_body, '$.action.parameters.flow_token') is not null then get_json_object(json_body, '$.action.parameters.flow_token')
        when get_json_object(json_body, '$.flow_token') is not null then get_json_object(json_body, '$.flow_token')
        end AS flow_token
    ,get_json_object(json_body, '$.wa_flow_response_params.flow_name') AS flow_name
    -- tipo1
    ,get_json_object(json_body, '$.action.name') AS action_name
    ,get_json_object(json_body, '$.action.parameters.flow_action') AS flow_action
    ,get_json_object(json_body, '$.action.parameters.flow_action_payload.screen') AS flow_action_payload_screen
    ,get_json_object(json_body, '$.action.parameters.flow_cta') AS flow_cta
    ,get_json_object(json_body, '$.action.parameters.flow_message_version') AS flow_message_version
    ,get_json_object(json_body, '$.header.text') AS header_text
    ,get_json_object(json_body, '$.header.type') AS header_type
    -- tipo2
    ,get_json_object(json_body, '$.type') AS type
    ,get_json_object(json_body, '$.complement_type') AS complement_type
    ,get_json_object(json_body, '$.street') AS street
    ,get_json_object(json_body, '$.number') AS number
    ,get_json_object(json_body, '$.neighborhood') AS neighborhood
    ,get_json_object(json_body, '$.city') AS city
    ,get_json_object(json_body, '$.uf') AS uf
    ,get_json_object(json_body, '$.zipcode') AS zipcode
    -- tipo3
    ,get_json_object(json_body, '$.selectedSecondDate') AS selectedSecondDate
    ,get_json_object(json_body, '$.selectedSecondPeriod') AS selectedSecondPeriod
    ,get_json_object(json_body, '$.selectedFirstDate') AS selectedFirstDate
    ,get_json_object(json_body, '$.selectedFirstPeriod') AS selectedFirstPeriod
    -- tipo4
    ,get_json_object(json_body, '$.complement') AS complement

    ,get_json_object(json_body, '$.wa_flow_response_params.title') AS flow_title

 from renamed
--where session_id not in ('cm4ixsgis006zjb5mdrcb9lkg','cm4iyx00s00ftjb5ma2ksvsoq')
--order by 3 desc
--where session_id in ('cm4kekfl704atjb5mk66502nk')