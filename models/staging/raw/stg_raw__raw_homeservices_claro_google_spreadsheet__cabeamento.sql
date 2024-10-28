with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_google_spreadsheet__cabeamento') }}

),

renamed as (

    select
        cep_num,
        classe_social,
        id,
        perfil,
        tipo_cidade,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
