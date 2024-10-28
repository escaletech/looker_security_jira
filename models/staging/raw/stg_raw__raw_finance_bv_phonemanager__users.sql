with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_phonemanager__users') }}

),

renamed as (

    select
        id,
        name,
        birth,
        identification,
        mail,
        type,
        occupation_type_id,
        username,
        password,
        password_updated_at,
        teams_id,
        department_id,
        system_profile_id,
        active,
        attendance,
        agent,
        peer,
        peer_fixed,
        account_code,
        workflow,
        account_password,
        external_id,
        external,
        phone,
        chapa,
        token,
        token_session,
        block_schedule,
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
