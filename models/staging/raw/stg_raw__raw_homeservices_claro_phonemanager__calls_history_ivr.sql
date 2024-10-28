with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__calls_history_ivr') }}

),

renamed as (

    select
        id,
        timestamp,
        call_id,
        token,
        ddd,
        phone,
        queue,
        status,
        ura_id,
        option_id,
        option_name,
        value,
        finalize,
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
