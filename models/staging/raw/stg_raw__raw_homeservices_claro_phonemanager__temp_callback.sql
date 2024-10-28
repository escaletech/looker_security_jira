with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__temp_callback') }}

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
        token_group,
        ddd,
        phone,
        first_status,
        last_status,
        number_attempts_cycle,
        number_attempts_full,
        number_cycles,
        is_wait_cycle,
        completed,
        duplicate,
        lock,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
