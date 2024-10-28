with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__calls_history_queue') }}

),

renamed as (

    select
        id,
        timestamp,
        team_id,
        team_description,
        user_id,
        username,
        agent,
        queue,
        queue_description,
        queue_type,
        queue_group_description,
        queue_child,
        lines_id,
        growth_history_id,
        call_id,
        token,
        token_group,
        ddd,
        phone,
        queue_log_line_types_id,
        phone_operator_id,
        phone_operator_description,
        queue_log_verb_types_id,
        server_id,
        position,
        origposition,
        wait,
        duration,
        duration_pos_attendance,
        queue_log_modality_types_id,
        locality,
        uf,
        channel_id,
        audio,
        dialling_id,
        created_at,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
