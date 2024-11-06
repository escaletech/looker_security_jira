{{ config(
    materialized='incremental',
    unique_key='_id'
) }}

with source as (
      select * from {{ source('raw', 'raw_finance_general_hubchat_escale_finance__messages') }}
      {% if is_incremental() %}
        where data > (select max(timestamp) from {{ this }})  -- Filtra apenas novos dados
      {% endif %}
),
renamed as (
    select
        {{ adapter.quote("__v") }},
        {{ adapter.quote("_id") }},
        {{ adapter.quote("data") }},
        {{ adapter.quote("lake_created_at") }},
        {{ adapter.quote("message_status") }},
        {{ adapter.quote("session_init") }},
        {{ adapter.quote("text") }},
        {{ adapter.quote("timestamp") }},
        {{ adapter.quote("token") }},
        {{ adapter.quote("user_id") }},
        {{ adapter.quote("who") }},
        {{ adapter.quote("year") }},
        {{ adapter.quote("month") }},
        {{ adapter.quote("day") }}
    from source
),
deduplicated as (
    {{ dbt_utils.deduplicate(
        relation=source('raw', 'raw_finance_general_hubchat_escale_finance__messages'),
        partition_by='_id',
        order_by="lake_created_at desc"
    ) }}
)
select * from deduplicated
