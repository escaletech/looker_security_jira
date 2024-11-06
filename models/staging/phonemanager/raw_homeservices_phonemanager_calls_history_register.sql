with source as (
      select * from {{ source('raw', 'raw_homeservices_claro_phonemanager__calls_history_register') }}
),
renamed as (
    select
        {{ adapter.quote("id") }},
        {{ adapter.quote("timestamp") }},
        {{ adapter.quote("call_id") }},
        {{ adapter.quote("queue") }},
        {{ adapter.quote("queue_child") }},
        {{ adapter.quote("queue_group") }},
        {{ adapter.quote("line_id") }},
        {{ adapter.quote("origin_channel_id") }},
        {{ adapter.quote("callback_log_verb") }},
        {{ adapter.quote("lead_id") }},
        {{ adapter.quote("token") }},
        {{ adapter.quote("ddd") }},
        {{ adapter.quote("phone") }},
        {{ adapter.quote("verb_id") }},
        {{ adapter.quote("ivr") }},
        {{ adapter.quote("cpf") }},
        {{ adapter.quote("server_id") }},
        {{ adapter.quote("completed") }},
        {{ adapter.quote("campaign") }},
        {{ adapter.quote("conversion_name") }},
        {{ adapter.quote("external_id") }},
        {{ adapter.quote("source") }},
        {{ adapter.quote("medium") }},
        {{ adapter.quote("fclid") }},
        {{ adapter.quote("gclid") }},
        {{ adapter.quote("updated_at") }},
        {{ adapter.quote("deleted_at") }},
        {{ adapter.quote("lake_created_at") }},
        {{ adapter.quote("year") }},
        {{ adapter.quote("month") }},
        {{ adapter.quote("day") }}

    from source
)
select * from renamed
  