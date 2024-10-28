with 

source as (

    select * from {{ source('raw', 'raw_homeservices_claro_google_search_console__queries') }}

),

renamed as (

    select
        clicks,
        ctr,
        domain,
        impressions,
        lake_created_at,
        position,
        name,
        date,
        query,
        month,
        day

    from source

)

select * from renamed
