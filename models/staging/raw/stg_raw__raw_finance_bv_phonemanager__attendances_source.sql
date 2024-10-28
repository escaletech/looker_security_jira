with 

source as (

    select * from {{ source('raw', 'raw_finance_bv_phonemanager__attendances_source') }}

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
        line_id,
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
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
