with 

source as (

    select * from {{ source('raw', 'raw_finance_general_hubspot__owners') }}

),

renamed as (

    select
        archived,
        created_at,
        email,
        first_name,
        id,
        lake_created_at,
        last_name,
        updated_at,
        user_id,
        team_id,
        team_name,
        team_primary,
        year,
        month,
        day

    from source

)

select * from renamed
