with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_phonemanager__teams_supervisors') }}

),

renamed as (

    select
        id,
        team_id,
        user_id,
        deleted_at,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
