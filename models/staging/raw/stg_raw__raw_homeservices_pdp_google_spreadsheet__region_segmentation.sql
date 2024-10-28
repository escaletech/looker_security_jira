with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_google_spreadsheet__region_segmentation') }}

),

renamed as (

    select
        segmentation,
        city,
        lake_created_at,
        period,
        state,
        year,
        month,
        day

    from source

)

select * from renamed
