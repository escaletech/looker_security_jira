with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_google_spreadsheet__finan_bp_crm') }}

),

renamed as (

    select
        cm,
        cm_rec_liquida,
        custo_projetado,
        data,
        deals_criado,
        producao_projetada,
        projecao_do_negocio,
        receita_liquida,
        share_projetado_crm,
        vendas_realizada,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
