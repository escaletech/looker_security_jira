with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_phonemanager__temp_callback') }}

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
        external_id,
        origin_channel_id,
        callback_log_verb,
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
