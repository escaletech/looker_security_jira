with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_google_spreadsheet__finan_cs_vendas') }}

),

renamed as (

    select
        2_clix,
        b2_c,
        cliente_apresentou_evidencias,
        consultor_argumentou,
        data_da_venda,
        data_monitoria,
        equipe,
        falha_grave,
        fez_ligacao,
        id_do_negocio,
        monitor,
        motivo_da_reducao_do__t,
        nome_do_cliente,
        nome_do_consultor,
        parceiro,
        procedencia,
        produto,
        proposta,
        qual_foi_o__t__utilizado,
        relatorio_da_monitoria,
        seguro_sppf,
        valor,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
