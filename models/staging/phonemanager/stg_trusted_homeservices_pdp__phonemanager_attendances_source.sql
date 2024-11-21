with 

source as (

    select * from {{ source('trusted_homeservices_pdp', 'phonemanager_attendances_source') }}

),

renamed as (

    select
        id,
        attendance_id,
        token,
        phone,
        recording,
        modality,
        queue_number,
        queue_child,
        queue_group_id,
        line_id,
        lead_id,
        source_mkt,
        medium_mkt,
        campaign_name_mkt,
        session_id_mkt,
        lead_source_mkt,
        simcard_source_mkt,
        main_connection,
        created_at,
        updated_at,
        deleted_at,
        hs_object_id,
        lake_created_at,
        year,
        month,
        day,
        lake_key

    from source

)

select 
    attendance_id
    , session_id_mkt message_session_id 
from renamed
