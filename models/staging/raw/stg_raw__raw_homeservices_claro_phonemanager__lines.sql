with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__lines') }}

),

renamed as (

    select
        id,
        number,
        number_0800,
        direct_id,
        schedule_id,
        channel_id,
        audio_busy,
        audio_timeout,
        is_0800,
        regenerate,
        callback,
        callback_fixo_ddd,
        callback_fixo_local,
        callback_movel_ddd,
        callback_movel_local,
        ivr,
        peer,
        queue_att,
        group_id,
        active,
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
