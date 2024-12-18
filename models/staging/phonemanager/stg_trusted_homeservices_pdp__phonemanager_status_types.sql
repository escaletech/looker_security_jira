with 

source as (

    select * from {{ source('trusted_homeservices_pdp', 'phonemanager_status_types') }}

),

renamed as (

    select
        id,
        description,
        reference,
        pattern,
        parent,
        clean_status,
        active,
        show,
        created_at,
        updated_at,
        deleted_at,
        lake_created_at,
        year,
        month,
        day,
        lake_key

    from source

)

select 
    id status_id
    ,upper(description) desc_status_attendance
from renamed