with source as (
    select
        *
    from {{ source('homeservices_pdp', 'google_ads_geo_target_constant') }}
)
, build_source as (
    SELECT
        geo_target_constant_id
        ,geo_target_constant_canonical_name
    FROM source
)
select
    *
from build_source