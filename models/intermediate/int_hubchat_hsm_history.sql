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
, cte_lateral_view as(
    SELECT
        message_session_id
        ,token
        ,case when col.key = 'null' then null else col.key end hsm 
        ,col.timestamp tsp_send_hsm
        , 0 flag_primeiro_hsm
    FROM cte_format
    LATERAL VIEW explode(parsed_array) exploded_data as col
)
, cte_first_hsm as(
    select 
        message_session_id
        , token
        , element_name_hsm hsm
        , timestamp tsp_send_hsm
        , 1 flag_primeiro_hsm
    from {{ ref('stg_trusted_homeservices_general__hubchat_chat_messages_context') }}
    where element_name_hsm is not null
    union all
    select 
        message_session_id
        , token
        , element_name_hsm hsm
        , timestamp tsp_send_hsm
        , 1 flag_primeiro_hsm
    from {{ ref('stg_trusted_finance_general__hubchat_escale_finance_messages_context') }}
    where element_name_hsm is not null
)
, cte_join_fist_history as (
    select * from cte_lateral_view
    union all
    select * from cte_first_hsm
)
,cte_main as (
SELECT
    message_session_id
    , w.* except(token,assistant_name)
    ,hsm 
    ,tsp_send_hsm
    ,flag_primeiro_hsm
FROM cte_join_fist_history h
left join {{ ref('int_join_hubchat_workspace') }} w on w.token = h.token
where hsm is not null
)
select * from cte_main
--where hsm like 'claro_dealers_digital_first_cep_num'