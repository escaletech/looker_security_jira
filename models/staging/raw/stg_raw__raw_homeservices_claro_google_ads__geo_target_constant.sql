with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_google_ads__geo_target_constant') }}

),

renamed as (

    select
        geo_target_constant_canonical_name,
        geo_target_constant_id,
        geo_target_constant_name,
        lake_created_at,
        business_id,
        month,
        day

    from source

)

select * from renamed
