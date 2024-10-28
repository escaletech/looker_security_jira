with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_phonemanager__queue') }}

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
