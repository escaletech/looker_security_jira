with 

source as (

    select * from {{ source('refined_homeservices_general', 'phonemanager_calls_history_register') }}

),

renamed as (

    select
        id,
        timestamp,
        call_id,
        queue,
        line_id,
        origin_channel_id,
        callback_log_verb,
        token,
        ddd,
        phone,
        cpf,
        server_id,
        completed,
        campaign,
        conversion_name,
        external_id,
        source,
        medium,
        fclid,
        gclid,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day,
        lake_key,
        brand,
        verb_id,
        ivr,
        queue_child,
        queue_group,
        lead_id

    from source

)

select * from renamed
