with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_google_spreadsheet__products_values') }}

),

renamed as (

    select
        lake_created_at,
        period,
        product_name_looker,
        product_name_oi,
        segment,
        value,
        year,
        month,
        day

    from source

)

select * from renamed
