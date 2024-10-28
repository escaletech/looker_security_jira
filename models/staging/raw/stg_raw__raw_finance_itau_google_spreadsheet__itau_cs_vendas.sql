with 

source as (

    select * from {{ source('raw', 'raw_finance_itau_google_spreadsheet__itau_cs_vendas') }}

),

renamed as (

    select
        2_clix,
        ade,
        consultor,
        data_monitoria,
        equipe,
        falha_grave,
        id_do_negocio,
        monitor,
        nome_do_cliente,
        parceiro,
        possivel_reclamacao,
        procedencia,
        produto,
        relatorio_da_monitoria,
        valor,
        lake_created_at,
        month,
        day

    from source

)

select * from renamed
