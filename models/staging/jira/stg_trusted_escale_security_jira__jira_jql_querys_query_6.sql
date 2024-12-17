with 

source as (

    select * from {{ source('trusted_escale_security_jira', 'jira_jql_querys_query_6') }}

),

renamed as (

    select
        asset,
        criticidade,
        data_de_criacao,
        issue_id,
        issue_key,
        jql_query,
        lake_created_at,
        lead_time,
        origem_1,
        responsavel,
        resumo,
        status,
        year,
        month,
        day,
        lake_key

    from source

)

select * from renamed
