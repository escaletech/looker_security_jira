with 

source as (

    select * from {{ source('raw', 'raw_finance_itau_google_spreadsheet__itau_bp_crm') }}

),

renamed as (

    select
        data,
        deals_criado,
        producao_projetada,
        share_projetado_crm,
        vendas_realizada,
        lake_created_at,
        month,
        day

    from source

)

select * from renamed
