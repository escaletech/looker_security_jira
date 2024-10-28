with 

source as (

    select * from {{ source('raw', 'raw_finance_itau_google_spreadsheet__fgts_ops_production_goals') }}

),

renamed as (

    select
        data,
        dia,
        lake_created_at,
        meta,
        meta_vendas,
        peso,
        supermeta,
        month,
        day

    from source

)

select * from renamed
