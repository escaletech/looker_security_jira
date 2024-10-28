with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_phonemanager__teams') }}

),

renamed as (

    select
        id,
        identification,
        description,
        agent_start,
        agent_end,
        unit_id,
        department_id,
        block_schedule,
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
