with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_phonemanager__calls_history_register') }}

),

renamed as (

    select
        id,
        timestamp,
        call_id,
        queue,
        line_id,
        queue_child,
        queue_group,
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
        day

    from source

)

select * from renamed
