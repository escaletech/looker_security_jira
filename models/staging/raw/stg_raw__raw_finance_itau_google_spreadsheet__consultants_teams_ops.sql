with 

source as (

    select * from {{ source('raw', 'raw_finance_itau_google_spreadsheet__consultants_teams_ops') }}

),

renamed as (

    select
        fim,
        inicio,
        lider,
        nome,
        safra,
        squad,
        treinamento,
        user_id,
        lake_created_at,
        month,
        day

    from source

)

select * from renamed
