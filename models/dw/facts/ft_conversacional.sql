with cte_join_tables as (
    select 
        * 
    from {{ ref('int_join_hubchat_messages') }}
)
,cte_metrics as (
    select 
        *
        ,CASE 
            WHEN options IS NOT NULL THEN options
            WHEN wpp_body IS NOT NULL THEN wpp_body
            WHEN response IS NOT NULL THEN response
            WHEN desc_message_text IS NOT NULL THEN desc_message_text 
            ELSE NULL 
        END AS text
        ,case when min(tsp_message) over(partition by message_session_id order by tsp_message) = tsp_message then 1 else 0 end as flag_first_msg
        ,case when max(tsp_message) over(partition by message_session_id order by tsp_message desc) = tsp_message then 1 else 0 end as flag_last_msg
        ,case when response_type = 'cron' then 1 else 0 end flag_timeout
    from cte_join_tables
)
select * from cte_metrics
WHERE (desc_message_source = "user" AND (UPPER(desc_message_text) LIKE '%CÓDIGO%' OR UPPER(desc_message_text) LIKE '%CODIGO%'))
or (desc_message_source = "user" AND (UPPER(response) LIKE '%CÓDIGO%' OR UPPER(response) LIKE '%CODIGO%'))