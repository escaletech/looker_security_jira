with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_google_spreadsheet__Forecast_growth_data') }}

),

renamed as (

    select
        week,
        custo_ops_cart,
        custo_ops_cdv,
        instalacao_cdv_oi,
        intalacao_cart_oi,
        lake_created_at,
        tm_cart_oi,
        tm_cdv_oi,
        year,
        month,
        day

    from source

)

select * from renamed
