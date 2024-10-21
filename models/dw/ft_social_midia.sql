with joins as (
select * from {{ ref('int_union_calculate_bing_ads') }}
union all
select * from {{ ref('int_union_calculate_facebook_ads') }}
union all
select * from {{ ref('int_union_calculate_google_ads') }}
)
select
    *
from joins
WHERE campaign_id IS NOT NULL