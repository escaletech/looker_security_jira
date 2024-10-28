with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_google_spreadsheet__auxiliary_intake') }}

),

renamed as (

    select
        segmentation,
        city,
        lake_created_at,
        period,
        product_name_looker,
        product_name_oi,
        segment,
        state,
        value,
        year,
        month,
        day

    from source

)

select * from renamed
