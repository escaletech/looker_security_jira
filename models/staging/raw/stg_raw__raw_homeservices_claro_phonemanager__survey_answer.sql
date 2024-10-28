with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__survey_answer') }}

),

renamed as (

    select
        id,
        timestamp,
        queue,
        answer,
        call_id,
        token,
        create_at,
        deleted_at,
        survey_question_id,
        lake_created_at,
        year,
        month,
        day

    from source

)

select * from renamed
