with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_google_spreadsheet__credito') }}

),

renamed as (

    select
        area,
        digito_retorno_rtdm,
        documento,
        marca_consulta_credito,
        operadora,
        ordem_aplicacao_regra,
        resultado_cliente_movel,
        resultado_consulta_credito_cliente_movel,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
