with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_google_spreadsheet__campaign_whatsapp') }}

),

renamed as (

    select
        campanha,
        canal,
        dominio,
        link,
        mensagem,
        mensagem_para_link,
        lake_created_at,
        month,
        day

    from source

)

select * from renamed
