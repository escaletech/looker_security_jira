with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__calls_history_register') }}

),

renamed as (

    select
        id,
        timestamp,
        call_id,
        queue,
        queue_child,
        queue_group,
        line_id,
        origin_channel_id,
        callback_log_verb,
        lead_id,
        token,
        ddd,
        phone,
        verb_id,
        ivr,
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
        day

    from source

)

select * from renamed