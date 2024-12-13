with 

source as (

    select * from {{ source('trusted_cross_general', 'typebot_session') }}

),

renamed as (

    select
        _id,
        await_user,
        bot_id,
        channel,
        closed_at,
        created_at,
        instance_id,
        lake_created_at,
        last_user_interaction_at,
        metadata_await_user,
        metadata_bot_id,
        metadata_context,
        metadata_created_at,
        metadata_id,
        metadata_instance_id,
        metadata_parameters_api_key,
        metadata_parameters_instance_name,
        metadata_parameters_owner_jid,
        metadata_parameters_push_name,
        metadata_parameters_remote_jid,
        metadata_parameters_server_url,
        metadata_push_name,
        metadata_remote_jid,
        metadata_session_id,
        metadata_status,
        metadata_type,
        metadata_updated_at,
        partner,
        remote_jid,
        session_id,
        status,
        updated_at,
        who,
        year,
        month,
        day,
        lake_key

    from source

)

select
    session_id message_session_id
    ,created_at
    ,closed_at
    ,partner
    ,metadata_push_name
    ,remote_jid
    ,status
    ,who as phone_number
    ,metadata_await_user
    ,await_user
 from renamed
