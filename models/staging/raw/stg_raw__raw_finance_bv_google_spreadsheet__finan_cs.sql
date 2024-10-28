with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_google_spreadsheet__finan_cs') }}

),

renamed as (

    select
        2_clix,
        b2_c__pj,
        comportamento,
        consultor_argumentou,
        data_monitoria,
        equipe,
        etapa_hubspot,
        falha_grave,
        id_do_negocio,
        macro_motivo,
        motivo_de_declinio_correto,
        motivo_declinio_consultor,
        nome_do_cliente,
        nome_do_consultor,
        parceiro,
        produto,
        qual_o_t,
        relatorio_da_monitoria,
        tabulacao,
        telefone,
        valor,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
