with 

source as (

    select * from {{ source('trusted_cross_general', 'typebot_agent') }}

),

renamed as (

    select
        _id,
        agent_id,
        created_at,
        description,
        lake_created_at,
        partner,
        partner_id,
        timeout_config_allowed_days,
        timeout_config_limit_send_window,
        timeout_config_message_text,
        timeout_config_name,
        timeout_config_send_end_hour,
        timeout_config_send_start_hour,
        timeout_config_timeout_after_abandon_in_minutes,
        timeout_config_type,
        year,
        month,
        day,
        lake_key

    from source

)

select * from renamed
