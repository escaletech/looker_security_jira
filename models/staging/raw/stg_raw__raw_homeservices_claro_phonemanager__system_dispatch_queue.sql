with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__system_dispatch_queue') }}

),

renamed as (

    select
        id,
        description,
        trigger_model,
        trigger_model_id,
        variable_values,
        action_type,
        attempt,
        error,
        available_at,
        dispatched_at,
        created_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
