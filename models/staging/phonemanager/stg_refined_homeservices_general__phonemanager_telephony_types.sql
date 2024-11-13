with 

source as (

    select * from {{ source('refined_homeservices_general', 'phonemanager_telephony_types') }}

),

renamed as (

    select
        id,
        description,
        reference,
        pattern,
        active,
        show,
        finalize,
        transfer,
        types,
        reports,
        created_at,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day,
        lake_key,
        brand,
        dialing,
        integrator

    from source

)

select * from renamed
