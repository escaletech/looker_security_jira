with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_hubspot__deals_pipelines') }}

),

renamed as (

    select
        archived,
        created_at,
        display_order,
        id,
        label,
        lake_created_at,
        updated_at,
        stage_archived,
        stage_created_at,
        stage_display_order,
        stage_id,
        stage_label,
        stage_metadata,
        stage_updated_at,
        stage_write_permissions,
        year,
        month,
        day

    from source

)

select * from renamed
