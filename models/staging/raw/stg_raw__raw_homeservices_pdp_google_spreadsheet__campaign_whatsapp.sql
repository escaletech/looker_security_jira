with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_google_spreadsheet__campaign_whatsapp') }}

),

renamed as (

    select
        campanha,
        codigo_protocolo,
        mensagem,
        url,
        lake_created_at,
        month,
        day

    from source

)

select * from renamed
