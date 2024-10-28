with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_phonemanager__users_teams') }}

),

renamed as (

    select
        id,
        user_id,
        team_id,
        rd,
        upd,
        active,
        created_at,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
