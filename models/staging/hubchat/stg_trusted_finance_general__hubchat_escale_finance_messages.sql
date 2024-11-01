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
        message_status

    from source

)

select * from renamed