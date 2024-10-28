with 

source as (

    select * from {{ source('raw', 'raw_finance_itau_google_spreadsheet__financiamento_ops_costs') }}

),

renamed as (

    select
        infra,
        ops,
        data,
        dia,
        lake_created_at,
        peso,
        month,
        day

    from source

)

select * from renamed
