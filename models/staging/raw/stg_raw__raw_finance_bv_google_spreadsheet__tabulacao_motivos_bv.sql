with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_google_spreadsheet__tabulacao_motivos_bv') }}

),

renamed as (

    select
        codigo_tabulacao,
        id,
        lake_created_at,
        motivo,
        produto,
        submotivo,
        year,
        month,
        day

    from source

)

select * from renamed
