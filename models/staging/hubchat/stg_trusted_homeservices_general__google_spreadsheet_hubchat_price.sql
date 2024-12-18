with 

source as (

    select * from {{ source('trusted_homeservices_general', 'google_spreadsheet_hubchat_price') }}

),

renamed as (

    select
        data_fim::date,
        data_ini::date,
        faixa_fim,
        faixa_ini,
        lake_created_at,
        valor,
        valor_fechado,
        year,
        month,
        day,
        lake_key

    from source

)

select * from renamed