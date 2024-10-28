with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_google_spreadsheet__base_cidades') }}

),

renamed as (

    select
        cidade,
        cod_operadora,
        ddd,
        ibge,
        regional,
        tipo_cabo,
        tipo_cidade,
        uf,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
