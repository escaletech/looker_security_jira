with 

source as (

    select * from {{ source('raw', 'raw_finance_general_google_spreadsheet__hubspot_teams') }}

),

renamed as (

    select
        business_associate,
        hubspot_team_id,
        hubspot_team_name,
        lake_created_at,
        month,
        day

    from source

)

select * from renamed
