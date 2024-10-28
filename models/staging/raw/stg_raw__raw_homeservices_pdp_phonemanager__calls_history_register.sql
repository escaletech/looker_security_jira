with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_phonemanager__calls_history_register') }}

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
        token,
        ddd,
        phone,
        ivr,
        verb_id,
        cpf,
        server_id,
        completed,
        campaign,
        conversion_name,
        external_id,
        lead_id,
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
