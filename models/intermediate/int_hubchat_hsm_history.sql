with cte_join as (
    select 
        message_session_id
        , token
        , history_hsm 
    from {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages_context') }}
    where history_hsm is not null
    union all
    select 
        message_session_id
        , token
        , history_hsm 
    from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_context') }}
    where history_hsm is not null
)
, cte_regexp as (
    select 
        message_session_id
        , token
        , regexp_replace(regexp_replace(regexp_replace(history_hsm,'\\{', '{"key": "'),', ', '", "timestamp": "'),'\\}', '"}') AS formatted_json
    from cte_join
)
, cte_format as(
    SELECT
        message_session_id
        , token
        , from_json(formatted_json, 'ARRAY<STRUCT<key: STRING, timestamp: STRING>>') AS parsed_array
    FROM cte_regexp
)
SELECT
    message_session_id
    , w.* except(token,assistant_name)
    ,case when col.key = 'null' then null else col.key end hsm 
    ,col.timestamp tsp_send_hsm
FROM cte_format
left join {{ ref('int_join_hubchat_workspace') }} w on w.token = cte_format.token
LATERAL VIEW explode(parsed_array) exploded_data as col