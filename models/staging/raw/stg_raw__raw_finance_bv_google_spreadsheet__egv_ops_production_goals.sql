with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_google_spreadsheet__egv_ops_production_goals') }}

),

renamed as (

    select
        data,
        dia,
        lake_created_at,
        meta,
        peso,
        year,
        month,
        day

    from source

)

select * from renamed
