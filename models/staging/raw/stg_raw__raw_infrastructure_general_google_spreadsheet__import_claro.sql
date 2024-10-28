with 

source as (

    select * from {{ source('raw', 'raw_infrastructure_general_google_spreadsheet__import_claro') }}

),

renamed as (

    select
        base,
        canceladas,
        data_recebimento,
        data_update,
        duplicadas,
        erro_contrato,
        erro_endereco,
        erro_operadora,
        habilitadas,
        lake_created_at,
        nao_localizadas,
        registros_base,
        registros_errados,
        month,
        day

    from source

)

select * from renamed
