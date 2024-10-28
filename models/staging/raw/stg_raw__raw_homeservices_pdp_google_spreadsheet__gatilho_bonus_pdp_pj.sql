with 

source as (

    select * from {{ source('raw', 'raw_homeservices_pdp_google_spreadsheet__gatilho_bonus_pdp_pj') }}

),

renamed as (

    select
        afiliados,
        brand,
        du,
        lake_created_at,
        meta_gross,
        meta_vll,
        periodo,
        prod_gross,
        prod_vll,
        year,
        month,
        day

    from source

)

select * from renamed
