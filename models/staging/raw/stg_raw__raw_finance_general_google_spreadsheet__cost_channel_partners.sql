with 

source as (

    select * from {{ source('raw', 'raw_finance_general_google_spreadsheet__cost_channel_partners') }}

),

renamed as (

    select
        contato_efetivo,
        data_fim,
        data_inicio,
        entrada_leads,
        interesse_demonstrado,
        lake_created_at,
        marca,
        negociacao,
        negociacao_aceita,
        partner,
        utm_campaign,
        utm_medium,
        utm_source,
        venda_finalizada,
        month,
        day

    from source

)

select * from renamed
