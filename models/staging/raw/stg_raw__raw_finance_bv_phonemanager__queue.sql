with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_phonemanager__queue') }}

),

renamed as (

    select
        id,
        number,
        server_id,
        queue_types_id,
        queue_group_types_id,
        description,
        identification,
        active,
        open_screen_attendance,
        direct,
        time_wait_direct,
        lines_id,
        announce,
        strategy_id,
        timeout,
        retry,
        music,
        context,
        maxlen,
        weight,
        wrapuptime,
        ringinuse,
        priority,
        using,
        ura,
        ura_type,
        peer_fixed,
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
