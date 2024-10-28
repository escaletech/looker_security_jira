with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_google_spreadsheet__auxiliary_intake') }}

),

renamed as (

    select
        lake_created_at,
        period,
        weight_days,
        year,
        month,
        day

    from source

)

select * from renamed
