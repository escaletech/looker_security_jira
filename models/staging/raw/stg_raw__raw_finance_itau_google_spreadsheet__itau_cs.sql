with 

source as (

    select * from {{ source('raw', 'raw_finance_itau_google_spreadsheet__itau_cs') }}

),

renamed as (

    select
        2_clix,
        data_monitoria,
        declinio_correto,
        etapa_hubspot,
        falha_grave,
        id_do_negocio,
        lider,
        macro_motivo,
        motivo_de_declinio_consultor,
        parceiro,
        produto,
        relatorio,
        tabulacao,
        telefone,
        valor,
        vendedor,
        lake_created_at,
        month,
        day

    from source

)

select * from renamed
