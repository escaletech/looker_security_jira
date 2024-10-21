with source as (
    select
        *
    from {{ source('homeservices_claro', 'google_ads_geographic_view') }}
)
, build_source as (
    SELECT
        CAST(segments_date AS DATE) AS reference_date,
        campaign_id,
        customer_id,
        business_id,
        CAST(REGEXP_REPLACE(segments_geo_target_city, '[^0-9]', '') AS INT) AS city_code
    FROM source
)
select
    *
from build_source