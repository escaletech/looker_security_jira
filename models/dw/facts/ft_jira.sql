with cte_union as (
    select * from {{ ref('stg_trusted_escale_security_jira__jira_jql_querys_query_1') }}
    union all
    select * from {{ ref('stg_trusted_escale_security_jira__jira_jql_querys_query_2') }}
    union all
    select * from {{ ref('stg_trusted_escale_security_jira__jira_jql_querys_query_3') }}
    union all
    select * from {{ ref('stg_trusted_escale_security_jira__jira_jql_querys_query_4') }}
    union all
    select * from {{ ref('stg_trusted_escale_security_jira__jira_jql_querys_query_5') }}
    union all
    select * from {{ ref('stg_trusted_escale_security_jira__jira_jql_querys_query_6') }}
)
select
    issue_id
    ,date_format(data_de_criacao,'yyyyMMdd') as data_id
    ,* except(year,month,day,lake_key,issue_id,lake_created_at)
from cte_union